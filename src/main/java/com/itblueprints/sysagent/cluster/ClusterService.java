package com.itblueprints.sysagent.cluster;

import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.job.JobExecService;
import com.itblueprints.sysagent.repository.RecordRepository;
import com.itblueprints.sysagent.scheduling.SchedulerService;
import com.itblueprints.sysagent.step.StepExecService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.itblueprints.sysagent.cluster.NodeRecord.MANAGER_ID;


@Component
@RequiredArgsConstructor
@Slf4j
public class ClusterService {

    private final RecordRepository repository;
    private final SchedulerService schedulerService;
    private final JobExecService jobExecService;
    private final StepExecService stepExecService;
    private final Config config;
    private final ThreadManager threadManager;

    //-------------------------
    NodeRecord nodeRecord = new NodeRecord();
    NodeRecord managerNodeRecord;

    //------------------------------------------
    @PostConstruct
    void init() {

        val hb = config.getHeartBeatSecs();
        nodeRecord.setStartedAt(System.currentTimeMillis());
        repository.save(nodeRecord);

        final Runnable r = () -> {
            try {
                onHeartBeat(hb);
            }
            catch (Exception e){
                e.printStackTrace();
                throw new SysAgentException("Error on heartbeat", e);
            }
        };
        log.debug("Starting cluster service with hearbeat = "+hb);
        scheduler.scheduleAtFixedRate(r, hb, hb, TimeUnit.SECONDS);
    }

    //------------------------------
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);

    //------------------------------
    void onHeartBeat(int heartBeatSecs){
        val timeNow = System.currentTimeMillis();
        val clusterInfo = computeClusterState(heartBeatSecs, timeNow);
        log.debug("HB - clusterInfo="+clusterInfo);

        if (!nodeRecord.isInitialised()) {
            if (clusterInfo.isManager) {
                schedulerService.initialise(clusterInfo);
            }
            jobExecService.initialise(clusterInfo); //All nodes need this to load up steps
            nodeRecord.setInitialised(true);
            repository.save(nodeRecord);
        } else {
            val now = LocalDateTime.now();
            if (clusterInfo.isManager) {
                schedulerService.onHeartBeat(clusterInfo, now);
                jobExecService.onHeartBeat(clusterInfo, now);
            }
            threadManager.getExecutor().submit(() -> stepExecService.onHeartBeat(clusterInfo, now));
        }
    }

    public static final int LEASE_HEARTBEATS = 2;

    private static final int CLEANUP_HEARTBEATS = 600;

    //----------------------------------------
    ClusterInfo computeClusterState(int heartBeatSecs, long timeNow){

        val hrtbt = heartBeatSecs * 1000;

        //check if cluster has been reset
        val ns = repository.getNodeRecordById(nodeRecord.getId());
        if(ns == null){
            nodeRecord.setInitialised(false);
            repository.save(nodeRecord);
        }

        //extend life
        nodeRecord.setAliveTill(timeNow + hrtbt * LEASE_HEARTBEATS);
        nodeRecord = repository.save(nodeRecord);
        val mgrNodeRecInDb = repository.getManagerNodeRecord();
        if(mgrNodeRecInDb == null){ //No leader record
            //create one. Only one will succeed as the id is constant and it has a unique constraint
            val mgrNodeRec = new NodeRecord();
            mgrNodeRec.setId(MANAGER_ID);
            mgrNodeRec.setManagerId(nodeRecord.getId());
            mgrNodeRec.setManagerSince(timeNow);
            mgrNodeRec.setManagerLeaseTill(timeNow + hrtbt * LEASE_HEARTBEATS);
            managerNodeRecord = repository.save(mgrNodeRec);
            nodeRecord.setManagerId(nodeRecord.getId());
        }
        else {  //Leader record exists
            //update state held
            managerNodeRecord = mgrNodeRecInDb;
            nodeRecord.setManagerId(managerNodeRecord.getManagerId());
            if(isManager()){ //This is the leader node
                //Extend lease
                managerNodeRecord.setManagerLeaseTill(timeNow + hrtbt * LEASE_HEARTBEATS);
                managerNodeRecord = repository.save(managerNodeRecord);
            }
            else { //Another node is the leader

                //If lease has expired
                if(managerNodeRecord.getManagerLeaseTill() < timeNow){
                    //Read leader record with lock
                    val lockedMgrNR = repository.tryGetLockedManagerNodeRecord();

                    //if was successful in obtaining the lock
                    if(lockedMgrNR != null) {
                        //Set this as the manager
                        managerNodeRecord.setManagerId(nodeRecord.getId());
                        managerNodeRecord.setManagerSince(timeNow);
                        managerNodeRecord.setManagerLeaseTill(timeNow + hrtbt * LEASE_HEARTBEATS);
                        managerNodeRecord.setLocked(false);
                        managerNodeRecord = repository.save(managerNodeRecord);
                    }
                }
            }
        }

        List deadNodeIds = new ArrayList<String>();
        if(isManager()) {
            //Handle dead nodes
            val otherNodeRecs = repository.getRecordsForOtherNodes(nodeRecord.getId()); //mongoTemplate.findAll(NodeRecord.class);

            for (val nodeRec : otherNodeRecs) {
                val nodeId = nodeRec.getId();
                //recently dead, notify
                if (nodeRec.getAliveTill() < timeNow - hrtbt * LEASE_HEARTBEATS){
                    deadNodeIds.add(nodeId);
                }
                //long dead, clean up
                if (nodeRec.getAliveTill() < timeNow - hrtbt * CLEANUP_HEARTBEATS){
                    repository.delete(nodeRec);
                }
            }
        }

        val ci = new ClusterInfo();
        ci.timeNow = Utils.toDateTime(timeNow);
        ci.nodeId = nodeRecord.getId();
        ci.isManager = isManager();
        ci.isBusy = threadManager.isNodeBusy();
        ci.deadNodeIds = deadNodeIds;
        return ci;
    }

    //----------------------
    public boolean isManager(){
        return managerNodeRecord.getManagerId().equals(nodeRecord.getId());
    }

}
