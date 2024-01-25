package com.itblueprints.sysagent.cluster;

import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.job.JobService;
import com.itblueprints.sysagent.scheduling.SchedulerService;
import com.itblueprints.sysagent.step.StepService;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Component
@RequiredArgsConstructor
@Slf4j
public class ClusterService {

    private final MongoTemplate mongoTemplate;
    private final SchedulerService schedulerService;
    private final JobService jobService;
    private final StepService stepService;
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
        val nodeInfo = computeNodeInfo(heartBeatSecs, timeNow);
        log.debug("* HB - Current nodeInfo = "+nodeInfo);

        if (!nodeRecord.isInitialised()) {
            if (nodeInfo.isManager) {
                schedulerService.initialise(nodeInfo);
            }
            jobService.initialise(nodeInfo); //All nodes need this to load up steps
            nodeRecord.setInitialised(true);
            mongoTemplate.save(nodeRecord);
        } else {
            val now = LocalDateTime.now();
            if (nodeInfo.isManager) {
                schedulerService.onHeartBeat(nodeInfo, now);
                jobService.onHeartBeat(nodeInfo, now);
            }
            stepService.onHeartBeat(nodeInfo, now);
        }
    }

    //----------------------------------------
    NodeInfo computeNodeInfo(int heartBeatSecs, long timeNow){

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
        nodeRecord.setAliveTill(timeNow + heartBeatSecs*1000);
        nodeRecord = mongoTemplate.save(nodeRecord);
        val savedMgrNS = mongoTemplate.findById(MANAGER_ID, NodeRecord.class);
        if(savedMgrNS == null){ //No leader record
            //create one. Only one will succeed as the id is constant and it has a unique constraint
            val mgrNS = new NodeRecord();
            mgrNS.setId(MANAGER_ID);
            mgrNS.setManagerId(nodeRecord.getId());
            mgrNS.setManagerSince(timeNow);
            mgrNS.setManagerLeaseTill(timeNow+heartBeatSecs*2000); //lease for 2 heart beats
            managerNodeRecord = mongoTemplate.save(mgrNS);
        }
        else {  //Leader record exists
            //update state held
            managerNodeRecord = savedMgrNS;

            if(isManager()){ //This is the leader node
                //Extend lease
                managerNodeRecord.setManagerLeaseTill(timeNow + heartBeatSecs*2000);
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
                    val lockedLeaderNS = mongoTemplate.findAndModify(mgrQuery, update, NodeRecord.class);

                    //if was successful in obtaining the lock
                    if(lockedLeaderNS != null) {
                        //Set this as the manager
                        managerNodeRecord.setManagerId(nodeRecord.getId());
                        managerNodeRecord.setManagerSince(timeNow);
                        managerNodeRecord.setManagerLeaseTill(timeNow + heartBeatSecs*2000);
                        managerNodeRecord.setLocked(false);
                        managerNodeRecord = mongoTemplate.save(managerNodeRecord);
                    }
                }
            }
        }

        val nodeInfo = new NodeInfo();
        nodeInfo.timeNow = Utils.toDateTime(timeNow);
        nodeInfo.thisNodeId = nodeRecord.getId();
        nodeInfo.isManager = isManager();
        nodeInfo.isBusy = threadManager.isNodeBusy();
        return nodeInfo;
    }

    //----------------------
    public boolean isManager(){
        return managerNodeRecord.getManagerId().equals(nodeRecord.getId());
    }

    public static final String MANAGER_ID = "M";

}
