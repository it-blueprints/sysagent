package com.itblueprints.sysagent.cluster;

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

    //-------------------------
    private NodeState nodeState;
    private NodeState managerNodeState;

    //Should never be more than 15
    public static final int HEARTBEAT_SECS = 10;

    //------------------------------------------
    @PostConstruct
    void init() {

        nodeState = new NodeState();
        nodeState.setStartedAt(System.currentTimeMillis());

        final Runnable heartBeat = () -> {
            try {
                onHeartBeat();
            }
            catch (Exception e){
                e.printStackTrace();
                throw new SystemAgentException("Error on heartbeat", e);
            }
        };
        hearbeatHandle = scheduler.scheduleAtFixedRate(heartBeat, HEARTBEAT_SECS, HEARTBEAT_SECS, TimeUnit.SECONDS);
    }

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);

    private ScheduledFuture<?> hearbeatHandle;

    //------------------------------
    void onHeartBeat(){
        val nodeInfo = computeNodeInfo();
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
    private NodeInfo computeNodeInfo(){
        val timeNow = System.currentTimeMillis();
        //extend life
        nodeState.setAliveTill(timeNow + HEARTBEAT_SECS*1000);
        mongoTemplate.save(nodeState);
        val leaderNs = mongoTemplate.findById(MANAGER_ID, NodeState.class);
        if(leaderNs == null){ //No leader record
            //create one. Only one will succeed as the id is constant and it has a unique constraint
            managerNodeState = newManagerNodeState(nodeState.getId(), timeNow);
            mongoTemplate.save(managerNodeState);
        }
        else {  //Leader record exists
            //update state held
            managerNodeState = leaderNs;

            if(isManager()){ //This is the leader node
                //Extend lease
                managerNodeState.setManagerLeaseTill(timeNow + HEARTBEAT_SECS*2000);
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
                        setNewManager(managerNodeState, nodeState.getId(), timeNow); //Set this as the leader
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
    private void setNewManager(NodeState mgrNode, String nodeId, long atTime){
        mgrNode.setManagerId(nodeId);
        mgrNode.setManagerSince(atTime);
        mgrNode.setManagerLeaseTill(atTime + HEARTBEAT_SECS*2000);
        mgrNode.setLocked(false);
    }

    //-----------------------------------------------------
    private NodeState newManagerNodeState(String nodeId, long timeNow){
        val mgr = new NodeState();
        mgr.setId(MANAGER_ID);
        mgr.setManagerId(nodeId);
        mgr.setManagerLeaseTill(timeNow+HEARTBEAT_SECS*2000); //lease for 2 heart beats
        return mgr;
    }

    public static final String MANAGER_ID = "M";

}
