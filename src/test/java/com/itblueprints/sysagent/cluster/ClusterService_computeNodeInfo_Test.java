package com.itblueprints.sysagent.cluster;

import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.job.JobService;
import com.itblueprints.sysagent.scheduling.SchedulerService;
import com.itblueprints.sysagent.step.StepService;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
class ClusterService_computeNodeInfo_Test {

    @Mock MongoTemplate mongoTemplate;
    @Mock SchedulerService schedulerService;
    @Mock JobService jobService;
    @Mock StepService stepService;
    @Mock Config config;

    ClusterService clusterService1;
    ClusterService clusterService2;
    ClusterService clusterService3;

    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        clusterService1 = new ClusterService(mongoTemplate, schedulerService, jobService, stepService, config);
        clusterService2 = new ClusterService(mongoTemplate, schedulerService, jobService, stepService, config);
        clusterService3 = new ClusterService(mongoTemplate, schedulerService, jobService, stepService, config);

        //Save puts in an id
        when(mongoTemplate.save(any())).thenAnswer(ans -> {
            val ns = (NodeState) ans.getArguments()[0];
            if(ns.getId() == null) {
                ns.setId(nextId());
            }
            return ns;
        });

    }

    //-------------------------------------
    @Test
    void computeNodeInfo() {

        //Node 1 start
        val node1_time1 = 1700000000000L;
        when(mongoTemplate.findById(ClusterService.MANAGER_ID, NodeState.class)).thenReturn(null);
        when(mongoTemplate.findAndModify(any(), any(), any())).thenReturn(null);
        clusterService1.nodeState.setStartedAt(node1_time1);
        val node1_result1 = clusterService1.computeNodeInfo(10, node1_time1);
        checkNodeInfo(node1_result1, "ID_1", true, node1_time1);
        checkNodeState(clusterService1, null, node1_time1+SECS_10);
        checkMgrNodeState(clusterService1, 0, "ID_1", node1_time1, node1_time1+SECS_20);

        //Node 1 Hb 2
        val node1_time2 = node1_time1 + SECS_10;
        var mgrNs = clusterService1.managerNodeState;
        when(mongoTemplate.findById(ClusterService.MANAGER_ID, NodeState.class)).thenReturn(mgrNs);
        when(mongoTemplate.findAndModify(any(), any(), any())).thenReturn(mgrNs);
        val node1_result2 = clusterService1.computeNodeInfo(10, node1_time2);
        checkNodeInfo(node1_result2, "ID_1", true, node1_time2);
        checkNodeState(clusterService1, null, node1_time2+SECS_10);
        checkMgrNodeState(clusterService1, 0, "ID_1", node1_time1, node1_time2+SECS_20);

        //Node 2 start
        val node2_time1 = node1_time2+SECS_1;
        val node_2_result1 = clusterService2.computeNodeInfo(10, node2_time1);
        checkNodeInfo(node_2_result1, "ID_2", false, node2_time1);
        checkNodeState(clusterService2, null, node2_time1+SECS_10);
        checkMgrNodeState(clusterService2, 0, "ID_1", node1_time1, node1_time2+SECS_20);

        //Node 2 Hb 2. By now mgr is unresponsive
        val node2_time2 = node2_time1+SECS_10;
        val node_2_result2 = clusterService2.computeNodeInfo(10, node2_time2);
        checkNodeInfo(node_2_result2, "ID_2", false, node2_time2);
        checkNodeState(clusterService2, null, node2_time2+SECS_10);
        checkMgrNodeState(clusterService2, 0, "ID_1", node1_time1, node1_time2+SECS_20);

        //Node 2 Hb 3. Expect this to become new mgr
        val node2_time3 = node2_time2+SECS_10;
        val node_2_result3 = clusterService2.computeNodeInfo(10, node2_time3);
        checkNodeInfo(node_2_result3, "ID_2", true, node2_time3);
        checkNodeState(clusterService2, null, node2_time3+SECS_10);
        checkMgrNodeState(clusterService2, 0, "ID_2", node2_time3, node2_time3+SECS_20);

        //Node 2 Hb 4. Node 2 is still the mgr
        val node2_time4 = node2_time3+SECS_10;
        val node_2_result4 = clusterService2.computeNodeInfo(10, node2_time4);
        checkNodeInfo(node_2_result4, "ID_2", true, node2_time4);
        checkNodeState(clusterService2, null, node2_time4+SECS_10);
        checkMgrNodeState(clusterService2, 0, "ID_2", node2_time3, node2_time4+SECS_20);

        mgrNs = clusterService2.managerNodeState;
        when(mongoTemplate.findById(ClusterService.MANAGER_ID, NodeState.class)).thenReturn(mgrNs);

        //Node 1 restarts
        val node1_time3 = node2_time4;
        clusterService3.nodeState.setStartedAt(node1_time3);
        val node1_result3 = clusterService3.computeNodeInfo(10, node1_time3);
        checkNodeInfo(node1_result3, "ID_3", false, node1_time3);
        checkNodeState(clusterService3, null, node1_time3+SECS_10);
        checkMgrNodeState(clusterService3, 0, "ID_2", node2_time3, node1_time3+SECS_20);

    }

    //-------------------------------------------
    private void checkNodeInfo(NodeInfo info, String id, boolean isMgr, long timeNow){
        assertEquals(id, info.thisNodeId);
        assertEquals(isMgr, info.isManager);
        assertEquals(Utils.toDateTime(timeNow), info.timeNow);
    }
    //---------------------------------------------------------
    private void checkNodeState(ClusterService svc, String mgrId, long aliveTill){
        assertEquals(mgrId, svc.nodeState.getManagerId());
        assertEquals(aliveTill, svc.nodeState.getAliveTill());
        assertEquals(0, svc.nodeState.getManagerSince());
        assertEquals(0, svc.nodeState.getManagerLeaseTill());
    }
    //-------------------------------------------------------------------
    private void checkMgrNodeState(ClusterService svc, long aliveTill, String mgrId, long mgrSince, long mgrLeaseTill){
        assertEquals("M", svc.managerNodeState.getId());
        assertEquals(aliveTill, svc.managerNodeState.getAliveTill());
        assertEquals(mgrId, svc.managerNodeState.getManagerId());
        assertEquals(mgrSince, svc.managerNodeState.getManagerSince());
        assertEquals(mgrLeaseTill, svc.managerNodeState.getManagerLeaseTill());
    }

    //-----------------
    private int id = 0;
    private String nextId(){
        id++;
        return "ID_"+id;
    }

    private static long SECS_10 = 10000;
    private static long SECS_20 = 20000;

    private static long SECS_1 = 1000;
}