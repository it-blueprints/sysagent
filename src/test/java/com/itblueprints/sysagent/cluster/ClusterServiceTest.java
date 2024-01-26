package com.itblueprints.sysagent.cluster;

import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.job.JobExecService;
import com.itblueprints.sysagent.scheduling.SchedulerService;
import com.itblueprints.sysagent.step.StepExecService;
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
class ClusterServiceTest {

    @Mock MongoTemplate mongoTemplate;
    @Mock SchedulerService schedulerService;
    @Mock
    JobExecService jobExecService;
    @Mock
    StepExecService stepExecService;
    @Mock Config config;
    @Mock ThreadManager threadManager;

    ClusterService clusterService1;
    ClusterService clusterService2;
    ClusterService clusterService3;

    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        clusterService1 = new ClusterService(mongoTemplate, schedulerService, jobExecService, stepExecService, config, threadManager);
        clusterService2 = new ClusterService(mongoTemplate, schedulerService, jobExecService, stepExecService, config, threadManager);
        clusterService3 = new ClusterService(mongoTemplate, schedulerService, jobExecService, stepExecService, config, threadManager);

        //Save puts in an id
        when(mongoTemplate.save(any())).thenAnswer(ans -> {
            val ns = (NodeRecord) ans.getArguments()[0];
            if(ns.getId() == null) {
                ns.setId(nextId());
            }
            return ns;
        });

    }

    //-------------------------------------
    //@Test
    void computeNodeInfo() {

        //Node 1 start
        val node1_time1 = 1700000000000L;
        when(mongoTemplate.findById(ClusterService.MANAGER_ID, NodeRecord.class)).thenReturn(null);
        when(mongoTemplate.findAndModify(any(), any(), any())).thenReturn(null);
        clusterService1.nodeRecord.setStartedAt(node1_time1);
        val node1_result1 = clusterService1.computeClusterState(10, node1_time1);
        checkNodeInfo(node1_result1, "ID_1", true, node1_time1);
        checkNodeState(clusterService1, null, node1_time1);
        checkMgrNodeState(clusterService1, node1_time1, "ID_1", node1_time1+SECS_20);

        //Node 1 Hb 2
        val node1_time2 = node1_time1 + SECS_10;
        var mgrNs = clusterService1.managerNodeRecord;
        when(mongoTemplate.findById(ClusterService.MANAGER_ID, NodeRecord.class)).thenReturn(mgrNs);
        when(mongoTemplate.findAndModify(any(), any(), any())).thenReturn(mgrNs);
        val node1_result2 = clusterService1.computeClusterState(10, node1_time2);
        checkNodeInfo(node1_result2, "ID_1", true, node1_time2);
        checkNodeState(clusterService1, null, node1_time2);
        checkMgrNodeState(clusterService1, node1_time2, "ID_1", node1_time2+SECS_30);

        //Node 2 start
        val node2_time1 = node1_time2+SECS_1;
        val node_2_result1 = clusterService2.computeClusterState(10, node2_time1);
        checkNodeInfo(node_2_result1, "ID_2", false, node2_time1);
        checkNodeState(clusterService2, null, node2_time1);
        checkMgrNodeState(clusterService2, node2_time1, "ID_1", node1_time2+SECS_20);

        //Node 2 Hb 2
        val node2_time2 = node2_time1+SECS_10;

        //Node 2 Hb 3. By now mgr is unresponsive
        val node2_time3 = node2_time2+SECS_10;
        val node_2_result3 = clusterService2.computeClusterState(10, node2_time3);
        checkNodeInfo(node_2_result3, "ID_2", false, node2_time3);
        checkNodeState(clusterService2, null, node2_time3);
        checkMgrNodeState(clusterService2, node2_time3, "ID_1", node1_time2+SECS_20);

        //Node 2 Hb 4. Expect this to become new mgr
        val node2_time4 = node2_time3+SECS_10;
        val node_2_result4 = clusterService2.computeClusterState(10, node2_time4);
        checkNodeInfo(node_2_result3, "ID_2", true, node2_time4);
        checkNodeState(clusterService2, null, node2_time4);
        checkMgrNodeState(clusterService2, node2_time4, "ID_2", node2_time3+SECS_20);

        //Node 2 Hb 4. Node 2 is still the mgr
        val node2_time5 = node2_time4+SECS_10;
        val node_2_result5 = clusterService2.computeClusterState(10, node2_time5);
        checkNodeInfo(node_2_result4, "ID_2", true, node2_time5);
        checkNodeState(clusterService2, null, node2_time5);
        checkMgrNodeState(clusterService2, node2_time5, "ID_2", node2_time4+SECS_20);

        mgrNs = clusterService2.managerNodeRecord;
        when(mongoTemplate.findById(ClusterService.MANAGER_ID, NodeRecord.class)).thenReturn(mgrNs);

        //Node 1 restarts
        val node1_time3 = node2_time4;
        clusterService3.nodeRecord.setStartedAt(node1_time3);
        val node1_result3 = clusterService3.computeClusterState(10, node1_time3);
        checkNodeInfo(node1_result3, "ID_3", false, node1_time3);
        checkNodeState(clusterService3, null, node1_time3);
        checkMgrNodeState(clusterService3, node1_time3, "ID_2", node1_time3+SECS_20);

    }

    //-------------------------------------------
    private void checkNodeInfo(ClusterState info, String id, boolean isMgr, long timeNow){
        assertEquals(id, info.nodeId);
        assertEquals(isMgr, info.isManager);
        assertEquals(Utils.toDateTime(timeNow), info.timeNow);
    }
    //---------------------------------------------------------
    private void checkNodeState(ClusterService svc, String mgrId, long timeNow){
        assertEquals(mgrId, svc.nodeRecord.getManagerId());
        assertEquals(timeNow + SECS_20, svc.nodeRecord.getAliveTill());
        assertEquals(0, svc.nodeRecord.getManagerSince());
        assertEquals(0, svc.nodeRecord.getManagerLeaseTill());
    }
    //-------------------------------------------------------------------
    private void checkMgrNodeState(ClusterService svc, long timeNow, String mgrId, long mgrSince){
        assertEquals("M", svc.managerNodeRecord.getId());
        assertEquals(timeNow + SECS_20, svc.managerNodeRecord.getAliveTill());
        assertEquals(mgrId, svc.managerNodeRecord.getManagerId());
        assertEquals(mgrSince, svc.managerNodeRecord.getManagerSince());
        assertEquals(timeNow + SECS_20, svc.managerNodeRecord.getManagerLeaseTill());
    }

    //-----------------
    private int id = 0;
    private String nextId(){
        id++;
        return "ID_"+id;
    }

    private static long SECS_10 = 10000;
    private static long SECS_20 = 20000;
    private static long SECS_30 = 20000;
    private static long SECS_1 = 1000;
}