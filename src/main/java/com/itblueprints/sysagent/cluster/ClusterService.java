package com.itblueprints.sysagent.cluster;

import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.job.JobExecutionService;
import com.itblueprints.sysagent.repository.RecordRepository;
import com.itblueprints.sysagent.scheduling.SchedulerService;
import com.itblueprints.sysagent.step.StepExecutionService;
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

/**
 * Controller for managing the state of the cluster. This includes tracking heartbeats,
 * determining which node is the manager and which ones are workers and signalling the
 * node is alive. Activities are carried out each time the ScheduledExecutorService
 * wakes up and runs
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ClusterService {

    private final RecordRepository repository;
    private final SchedulerService schedulerService;
    private final JobExecutionService jobExecutionService;
    private final StepExecutionService stepExecutionService;
    private final Config config;
    private final ThreadManager threadManager;

    //-------------------------
    NodeRecord nodeRecord = new NodeRecord();
    NodeRecord managerNodeRecord;

    //-----------------------------------------------------
    /**
     * Initialises the scheduled executor which then triggers every
     * heartbeat number of seconds
     */
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

    //----------------------------------------------------
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);

    //------------------------------------------------------------------
    /**
     * Called by the scheduled executor everytime heartBeatSecs elapses.
     * Main method that carries out all the cluster related activities
     * @param heartBeatSecs the number of seconds between each heart beat
     */
    void onHeartBeat(int heartBeatSecs){
        val timeNow = System.currentTimeMillis();
        //
        val clusterInfo = computeClusterInfo(heartBeatSecs, timeNow);
        log.debug("HB - clusterInfo="+clusterInfo);

        if (!nodeRecord.isInitialised()) {
            if (clusterInfo.isManager) {
                schedulerService.initialise(clusterInfo);
            }
            jobExecutionService.initialise(clusterInfo); //All nodes need this to load up steps
            nodeRecord.setInitialised(true);
            repository.save(nodeRecord);
        } else {
            val now = LocalDateTime.now();
            if (clusterInfo.isManager) {
                schedulerService.onHeartBeat(clusterInfo, now);
                jobExecutionService.onHeartBeat(clusterInfo, now);
            }
            threadManager.getExecutor().submit(() -> stepExecutionService.onHeartBeat(clusterInfo, now));
        }
    }

    public static final int LEASE_HEARTBEATS = 2;

    private static final int CLEANUP_HEARTBEATS = 600;

    //---------------------------------------------------------------
    /**
     * Figures out the state of the cluster
     * @param heartBeatSecs the number of seconds between each heart beat
     * @param timeNow A long representing the time now
     * @return A ClusterInfo object that hold information about the state of the cluster
     */
    ClusterInfo computeClusterInfo(int heartBeatSecs, long timeNow){

        val hrtbt = heartBeatSecs * 1000;

        //check if a NodeRecord for this node exists. If not create one and save to db
        val ns = repository.getNodeRecordById(nodeRecord.getId());
        if(ns == null){
            nodeRecord.setInitialised(false);
            repository.save(nodeRecord);
        }

        //extend life lease
        nodeRecord.setAliveTill(timeNow + hrtbt * LEASE_HEARTBEATS);
        nodeRecord = repository.save(nodeRecord);

        //Get the manager record from the DB. This is the NodeRecord with id='M'
        val savedMgrNodeRec = repository.getManagerNodeRecord();
        if(savedMgrNodeRec == null){ //No manager record
            //create one. Only one node will succeed it creating it. This is beacuse
            // the id is the constant 'M' and that would violate the unique constraint
            val mgrNodeRec = new NodeRecord();
            mgrNodeRec.setId(MANAGER_ID);
            mgrNodeRec.setManagerNodeId(nodeRecord.getId());
            mgrNodeRec.setManagerSince(timeNow);
            mgrNodeRec.setManagerLeaseTill(timeNow + hrtbt * LEASE_HEARTBEATS);
            repository.save(mgrNodeRec); //This call may fail silently
        }

        managerNodeRecord = repository.getManagerNodeRecord();

        if(isManager()){ //This is the manager node
            //Extend lease
            managerNodeRecord.setManagerLeaseTill(timeNow + hrtbt * LEASE_HEARTBEATS);
            managerNodeRecord = repository.save(managerNodeRecord);
        }
        else { //Another node is the manager

            //If lease has expired
            if(managerNodeRecord.getManagerLeaseTill() < timeNow){
                //Read manager record with lock
                val lockedMgrNR = repository.tryGetLockedManagerNodeRecord();

                //if was successful in obtaining the lock
                if(lockedMgrNR != null) {
                    //Set this as the manager
                    managerNodeRecord.setManagerNodeId(nodeRecord.getId());
                    managerNodeRecord.setManagerSince(timeNow);
                    managerNodeRecord.setManagerLeaseTill(timeNow + hrtbt * LEASE_HEARTBEATS);
                    managerNodeRecord.setLocked(false);
                    managerNodeRecord = repository.save(managerNodeRecord);
                }
            }
        }


        //Handle any dead nodes
        List deadNodeIdList = new ArrayList<String>();
        if(isManager()) {

            val otherNodeRecs = repository.getRecordsForOtherNodes(nodeRecord.getId());

            for (val nodeRec : otherNodeRecs) {
                val nodeId = nodeRec.getId();
                //recently dead, notify
                if (nodeRec.getAliveTill() < timeNow - hrtbt * LEASE_HEARTBEATS){
                    deadNodeIdList.add(nodeId);
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
        ci.deadNodeIds = deadNodeIdList;
        return ci;
    }

    //----------------------
    public boolean isManager(){
        return managerNodeRecord.getManagerNodeId().equals(nodeRecord.getId());
    }

}
