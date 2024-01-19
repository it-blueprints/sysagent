package com.itblueprints.sysagent.cluster;

import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.SystemAgentException;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.job.JobService;
import com.itblueprints.sysagent.step.StepService;
import com.itblueprints.sysagent.scheduling.SchedulerService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


@Component
@RequiredArgsConstructor
public class ClusterService {

    private final MongoTemplate mongoTemplate;
    private final SchedulerService schedulerService;
    private final JobService jobService;
    private final StepService stepService;
    private final Config config;

    //-------------------------
    private NodeState nodeState;
    private NodeState managerNodeState;

    //------------------------------------------
    @PostConstruct
    void init() {

        val hb = config.getHeartBeatSecs();
        nodeState = new NodeState();
        nodeState.setStartedAt(System.currentTimeMillis());

        final Runnable r = () -> {
            try {
                onHeartBeat(hb);
            }
            catch (Exception e){
                e.printStackTrace();
                throw new SystemAgentException("Error on heartbeat", e);
            }
        };
        hearbeatHandle = scheduler.scheduleAtFixedRate(r, hb, hb, TimeUnit.SECONDS);
    }

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);

    private ScheduledFuture<?> hearbeatHandle;

    //------------------------------
    void onHeartBeat(int heartBeatSecs){
        val nodeInfo = computeNodeInfo(heartBeatSecs);
        if(!isInitialised) {
            if(nodeInfo.isManager) {
                schedulerService.initialise(nodeInfo);
            }
            jobService.initialise(nodeInfo);
            isInitialised = true;
        }
        else {

            if (nodeInfo.isManager) {
                schedulerService.onHeartBeat(nodeInfo);
            }
            jobService.onHeartBeat(nodeInfo);
            stepService.onHeartBeat(nodeInfo);
        }

    }

    private boolean isInitialised = false;

    //----------------------------------------
    private NodeInfo computeNodeInfo(int heartBeatSecs){
        val timeNow = System.currentTimeMillis();
        //extend life
        nodeState.setAliveTill(timeNow + heartBeatSecs*1000);
        mongoTemplate.save(nodeState);
        val leaderNs = mongoTemplate.findById(MANAGER_ID, NodeState.class);
        if(leaderNs == null){ //No leader record
            //create one. Only one will succeed as the id is constant and it has a unique constraint
            managerNodeState = newManagerNodeState(nodeState.getId(), timeNow, heartBeatSecs);
            mongoTemplate.save(managerNodeState);
        }
        else {  //Leader record exists
            //update state held
            managerNodeState = leaderNs;

            if(isManager()){ //This is the leader node
                //Extend lease
                managerNodeState.setManagerLeaseTill(timeNow + heartBeatSecs*2000);
                mongoTemplate.save(managerNodeState);

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
                        setNewManager(managerNodeState, nodeState.getId(), timeNow, heartBeatSecs); //Set this as the leader
                        mongoTemplate.save(managerNodeState);
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


    //----------------------------------------------
    private void setNewManager(NodeState mgrNode, String nodeId, long atTime, int heartBeatSecs){
        mgrNode.setManagerId(nodeId);
        mgrNode.setManagerSince(atTime);
        mgrNode.setManagerLeaseTill(atTime + heartBeatSecs*2000);
        mgrNode.setLocked(false);
    }

    //-----------------------------------------------------
    private NodeState newManagerNodeState(String nodeId, long timeNow, int heartBeatSecs){
        val mgr = new NodeState();
        mgr.setId(MANAGER_ID);
        mgr.setManagerId(nodeId);
        mgr.setManagerLeaseTill(timeNow+heartBeatSecs*2000); //lease for 2 heart beats
        return mgr;
    }

    public static final String MANAGER_ID = "M";

}
