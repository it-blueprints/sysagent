package com.itblueprints.sysagent.cluster;

import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.job.JobService;
import com.itblueprints.sysagent.step.StepService;
import com.itblueprints.sysagent.scheduling.SchedulerService;
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
import java.util.concurrent.ScheduledFuture;
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

    //-------------------------
    NodeState nodeState = new NodeState();
    NodeState managerNodeState;

    //------------------------------------------
    @PostConstruct
    void init() {

        val hb = config.getHeartBeatSecs();
        nodeState.setStartedAt(System.currentTimeMillis());

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
        hearbeatHandle = scheduler.scheduleAtFixedRate(r, hb, hb, TimeUnit.SECONDS);
    }

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);

    private ScheduledFuture<?> hearbeatHandle;

    //------------------------------
    void onHeartBeat(int heartBeatSecs){
        val timeNow = System.currentTimeMillis();
        val nodeInfo = computeNodeInfo(heartBeatSecs, timeNow);
        log.debug("Current nodeInfo = "+nodeInfo);
        if(!isInitialised) {
            if(nodeInfo.isManager) {
                schedulerService.initialise(nodeInfo);
            }
            jobService.initialise(nodeInfo);
            isInitialised = true;
        }
        else {
            if (nodeInfo.isManager) {
                schedulerService.onHeartBeat(nodeInfo, LocalDateTime.now());
            }
            jobService.onHeartBeat(nodeInfo, LocalDateTime.now());
            stepService.onHeartBeat(nodeInfo, LocalDateTime.now());
        }

    }

    private boolean isInitialised = false;

    //----------------------------------------
    NodeInfo computeNodeInfo(int heartBeatSecs, long timeNow){

        //extend life
        nodeState.setAliveTill(timeNow + heartBeatSecs*1000);
        nodeState = mongoTemplate.save(nodeState);
        val savedMgrNS = mongoTemplate.findById(MANAGER_ID, NodeState.class);
        if(savedMgrNS == null){ //No leader record
            //create one. Only one will succeed as the id is constant and it has a unique constraint
            val mgrNS = new NodeState();
            mgrNS.setId(MANAGER_ID);
            mgrNS.setManagerId(nodeState.getId());
            mgrNS.setManagerSince(timeNow);
            mgrNS.setManagerLeaseTill(timeNow+heartBeatSecs*2000); //lease for 2 heart beats
            managerNodeState = mongoTemplate.save(mgrNS);
        }
        else {  //Leader record exists
            //update state held
            managerNodeState = savedMgrNS;

            if(isManager()){ //This is the leader node
                //Extend lease
                managerNodeState.setManagerLeaseTill(timeNow + heartBeatSecs*2000);
                managerNodeState = mongoTemplate.save(managerNodeState);

            }
            else { //Another node is the leader

                //If lease has expired
                if(managerNodeState.getManagerLeaseTill() < timeNow){
                    //Read leader record with lock
                    val query = new Query();
                    query.addCriteria(Criteria
                            .where("id").is(MANAGER_ID)
                            .and("locked").is(false));
                    val update = new Update();
                    update.set("locked", true);
                    val lockedLeaderNS = mongoTemplate.findAndModify(query, update, NodeState.class);

                    //if was successful in obtaining the lock
                    if(lockedLeaderNS != null) {
                        //Set this as the manager
                        managerNodeState.setManagerId(nodeState.getId());
                        managerNodeState.setManagerSince(timeNow);
                        managerNodeState.setManagerLeaseTill(timeNow + heartBeatSecs*2000);
                        managerNodeState.setLocked(false);
                        managerNodeState = mongoTemplate.save(managerNodeState);
                    }
                }
            }
        }

        val clusterState = new NodeInfo();
        clusterState.timeNow = Utils.millisToDateTime(timeNow);
        clusterState.thisNodeId = nodeState.getId();
        clusterState.isManager = isManager();
        clusterState.isBusy = false;
        return clusterState;
    }

    //----------------------
    public boolean isManager(){
        return managerNodeState.getManagerId().equals(nodeState.getId());
    }

    public static final String MANAGER_ID = "M";

}
