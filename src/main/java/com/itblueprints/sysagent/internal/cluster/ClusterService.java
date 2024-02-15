/*
 * Copyright 2023-2024 the original author or authors.
 *
 * Licensed under the Apache Software License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.itblueprints.sysagent.internal.cluster;

import com.itblueprints.sysagent.internal.Config;
import com.itblueprints.sysagent.internal.SysAgentException;
import com.itblueprints.sysagent.internal.ThreadManager;
import com.itblueprints.sysagent.internal.Utils;
import com.itblueprints.sysagent.internal.job.JobExecutionService;
import com.itblueprints.sysagent.internal.repository.RecordRepository;
import com.itblueprints.sysagent.internal.scheduling.SchedulerService;
import com.itblueprints.sysagent.internal.step.StepExecutionService;
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
    ManagerNodeRecord managerNodeRecord;

    private boolean isInitialised = false;

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
        log.info("Starting cluster service with heartBeatSecs = "+hb);
        scheduler.scheduleAtFixedRate(r, hb, hb, TimeUnit.SECONDS);
    }

    //----------------------------------------------------
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);


    /**
     * Called by the scheduled executor everytime heartBeatSecs elapses.
     * Main method that carries out all the cluster related activities
     * @param heartBeatSecs the number of seconds between each heart beat
     */
    void onHeartBeat(int heartBeatSecs){
        val timeNow = System.currentTimeMillis();

        //Get the node info
        val nodeInfo = computeNodeInfo(heartBeatSecs, timeNow);
        log.debug("HB - nodeInfo="+nodeInfo);

        if (!isInitialised) {
            if (nodeInfo.isManager) {
                schedulerService.initialise(nodeInfo);
            }
            jobExecutionService.initialise(nodeInfo); //All nodes need this to load up steps
            isInitialised = true;
            repository.save(nodeRecord);
        } else {
            val now = LocalDateTime.now();
            if (nodeInfo.isManager) {
                schedulerService.onHeartBeat(nodeInfo, now);
                jobExecutionService.onHeartBeat(nodeInfo, now);
            }
            threadManager.getExecutor().submit(() -> stepExecutionService.onHeartBeat(nodeInfo, now));
        }
    }

    public static final int LEASE_HEARTBEATS = 2;

    private static final int CLEANUP_HEARTBEATS = 600;


    /**
     * Figures out the state of the cluster
     * @param heartBeatSecs the number of seconds between each heart beat
     * @param timeNow A long representing the time now
     * @return A NodeInfo that holds information about the state of the node and the cluster
     */
    NodeInfo computeNodeInfo(int heartBeatSecs, long timeNow){

        val hrtbt = heartBeatSecs * 1000;

        //check if a NodeRecord for this node exists. If not create one and save to db
        val ns = repository.getNodeRecordById(nodeRecord.getId());

        //extend life lease
        nodeRecord.setLifeLeaseTill(timeNow + hrtbt * LEASE_HEARTBEATS);
        nodeRecord = repository.save(nodeRecord);

        //Get the manager record from the DB. This is the NodeRecord with id='M'
        val savedMgrNodeRec = repository.getManagerNodeRecord();
        if(savedMgrNodeRec == null){ //No manager record
            //create one. Only one node will succeed it creating it. This is beacuse
            // the id is the constant 'M' and that would violate the unique constraint
            val mgrNodeRec = new ManagerNodeRecord();
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
                if (nodeRec.getLifeLeaseTill() < timeNow - hrtbt * LEASE_HEARTBEATS){
                    deadNodeIdList.add(nodeId);
                }
                //long dead, clean up
                if (nodeRec.getLifeLeaseTill() < timeNow - hrtbt * CLEANUP_HEARTBEATS){
                    repository.delete(nodeRec);
                }
            }
        }

        //Finally return the result
        val ci = new NodeInfo();
        ci.timeNow = Utils.toDateTime(timeNow);
        ci.nodeId = nodeRecord.getId();
        ci.isManager = isManager();
        ci.isNodeBusy = threadManager.isNodeBusy();
        ci.deadNodeIds = deadNodeIdList;
        return ci;
    }

    /**
     * Checks if this node is the manager by comparing its own NodeRecord with
     * the manager NodeRecord
     * @return If this node is the manager
     */
    public boolean isManager(){
        return managerNodeRecord.getManagerNodeId().equals(nodeRecord.getId());
    }

}
