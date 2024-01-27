package com.itblueprints.sysagent.cluster;

import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.job.JobExecService;
import com.itblueprints.sysagent.scheduling.SchedulerService;
import com.itblueprints.sysagent.step.StepExecService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Component
@RequiredArgsConstructor
@Slf4j
public class ClusterService {

    private final MongoTemplate mongoTemplate;
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
        mongoTemplate.save(nodeRecord);

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
        val nodeInfo = computeClusterState(heartBeatSecs, timeNow);
        log.debug("* HB - Current nodeInfo = "+nodeInfo);

        if (!nodeRecord.isInitialised()) {
            if (nodeInfo.isManager) {
                schedulerService.initialise(nodeInfo);
            }
            jobExecService.initialise(nodeInfo); //All nodes need this to load up steps
            nodeRecord.setInitialised(true);
            mongoTemplate.save(nodeRecord);
        } else {
            val now = LocalDateTime.now();
            if (nodeInfo.isManager) {
                schedulerService.onHeartBeat(nodeInfo, now);
                jobExecService.onHeartBeat(nodeInfo, now);
            }
            threadManager.getExecutor().submit(() -> stepExecService.onHeartBeat(nodeInfo, now));
        }
    }

    public static final int LEASE_HEARTBEATS = 2;

    private static final int CLEANUP_HEARTBEATS = 600;

    //----------------------------------------
    ClusterState computeClusterState(int heartBeatSecs, long timeNow){

        val hrtbt = heartBeatSecs * 1000;

        //check if cluster has been reset
        val query = new Query();
        query.addCriteria(Criteria
                .where("id").is(nodeRecord.getId()));
        val ns = mongoTemplate.findOne(query, NodeRecord.class);
        if(ns == null){
            nodeRecord.setInitialised(false);
            mongoTemplate.save(nodeRecord);
        }

        //extend life
        nodeRecord.setAliveTill(timeNow + hrtbt * LEASE_HEARTBEATS);
        nodeRecord = mongoTemplate.save(nodeRecord);
        val mgrNodeRecInDb = mongoTemplate.findById(MANAGER_ID, NodeRecord.class);
        if(mgrNodeRecInDb == null){ //No leader record
            //create one. Only one will succeed as the id is constant and it has a unique constraint
            val mgrNodeRec = new NodeRecord();
            mgrNodeRec.setId(MANAGER_ID);
            mgrNodeRec.setManagerId(nodeRecord.getId());
            mgrNodeRec.setManagerSince(timeNow);
            mgrNodeRec.setManagerLeaseTill(timeNow + hrtbt * LEASE_HEARTBEATS);
            managerNodeRecord = mongoTemplate.save(mgrNodeRec);
            nodeRecord.setManagerId(nodeRecord.getId());
        }
        else {  //Leader record exists
            //update state held
            managerNodeRecord = mgrNodeRecInDb;
            nodeRecord.setManagerId(managerNodeRecord.getManagerId());
            if(isManager()){ //This is the leader node
                //Extend lease
                managerNodeRecord.setManagerLeaseTill(timeNow + hrtbt * LEASE_HEARTBEATS);
                managerNodeRecord = mongoTemplate.save(managerNodeRecord);
            }
            else { //Another node is the leader

                //If lease has expired
                if(managerNodeRecord.getManagerLeaseTill() < timeNow){
                    //Read leader record with lock
                    val mgrQuery = new Query();
                    mgrQuery.addCriteria(Criteria
                            .where("id").is(MANAGER_ID)
                            .and("locked").is(false));
                    val update = new Update();
                    update.set("locked", true);
                    val lockedMgrNR = mongoTemplate.findAndModify(mgrQuery, update, NodeRecord.class);

                    //if was successful in obtaining the lock
                    if(lockedMgrNR != null) {
                        //Set this as the manager
                        managerNodeRecord.setManagerId(nodeRecord.getId());
                        managerNodeRecord.setManagerSince(timeNow);
                        managerNodeRecord.setManagerLeaseTill(timeNow + hrtbt * LEASE_HEARTBEATS);
                        managerNodeRecord.setLocked(false);
                        managerNodeRecord = mongoTemplate.save(managerNodeRecord);
                    }
                }
            }
        }

        List deadNodeIds = new ArrayList<String>();
        if(isManager()) {
            //Handle dead nodes
            val allNodes = mongoTemplate.findAll(NodeRecord.class);

            for (val node : allNodes) {
                val nodeId = node.getId();
                if (nodeId.equals(MANAGER_ID) || nodeId.equals(nodeRecord.getId())) continue;
                //recently dead, notify
                if (node.getAliveTill() < timeNow - hrtbt * LEASE_HEARTBEATS){
                    deadNodeIds.add(nodeId);
                }
                //long dead, clean up
                if (node.getAliveTill() < timeNow - hrtbt * CLEANUP_HEARTBEATS){
                    mongoTemplate.remove(node);
                }
            }
        }

        val cs = new ClusterState();
        cs.timeNow = Utils.toDateTime(timeNow);
        cs.nodeId = nodeRecord.getId();
        cs.isManager = isManager();
        cs.isBusy = threadManager.isNodeBusy();
        cs.deadNodeIds = deadNodeIds;
        return cs;
    }

    //----------------------
    public boolean isManager(){
        return managerNodeRecord.getManagerId().equals(nodeRecord.getId());
    }

    public static final String MANAGER_ID = "M";

}
