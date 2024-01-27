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

import java.util.List;

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

    List<ClusterService> nodes;

    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        nodes = List.of(
           new ClusterService(mongoTemplate, schedulerService, jobExecService, stepExecService, config, threadManager),
           new ClusterService(mongoTemplate, schedulerService, jobExecService, stepExecService, config, threadManager),
           new ClusterService(mongoTemplate, schedulerService, jobExecService, stepExecService, config, threadManager)
        );

        //Save puts in an id
        when(mongoTemplate.save(any())).thenAnswer(ans -> {
            val ns = (NodeRecord) ans.getArguments()[0];
            if(ns.getId() == null) {
                ns.setId(nextId());
            }
            return ns;
        });
    }

    private static final int HB_SECS = 10;
    private static final long START_TIME = 1700000000000L;
    private static final long LEASE_MILLIS = ClusterService.LEASE_HEARTBEATS*HB_SECS*1000;

    private boolean clusterInited = false;
    private NodeRecord mgrNodeRecord;
    //-------------------------------------
    @Test
    void computeClusterState() {

        val testCases = List.of(
                TestCase.of(0, 0, true),
                TestCase.of(1, 1, false),
                TestCase.of(10, 0, true),
                TestCase.of(11, 1, false),
                TestCase.of(31, 1, true),
                TestCase.of(31, 2, false),
                TestCase.of(32, 0, false),
                TestCase.of(41, 2, false),
                TestCase.of(51, 2, false),
                TestCase.of(52, 0, true),
                TestCase.of(61, 2, false)
        );

        val i = new int[]{1};
        testCases.stream().forEach( tc -> {
                    System.out.println("Test case # " + i[0]);
                    mgrNodeRecord = assertState(tc, i[0]);
                    i[0]++;
                });
    }
    //-------------------------------------------------
    private NodeRecord assertState(TestCase testCase, int testNum){

        val timeNow = START_TIME + testCase.sec * 1000;

        val nIdx = testCase.nodeIdx;
        val nodeId = "ID_" + (nIdx + 1);
        val node = nodes.get(nIdx);

        if(!clusterInited) {
            when(mongoTemplate.findById(ClusterService.MANAGER_ID, NodeRecord.class)).thenReturn(null);
            when(mongoTemplate.findAndModify(any(), any(), any())).thenReturn(null);
            node.nodeRecord.setStartedAt(timeNow);
            clusterInited = true;
        }
        else{
            when(mongoTemplate.findById(ClusterService.MANAGER_ID, NodeRecord.class)).thenReturn(mgrNodeRecord);
            lenient().when(mongoTemplate.findAndModify(any(), any(), any())).thenReturn(mgrNodeRecord);
        }

        val cs = node.computeClusterState(HB_SECS, timeNow);

        //cluster state
        assertEquals(nodeId, cs.nodeId);
        assertEquals(testCase.shouldBeManager, cs.isManager);
        assertEquals(Utils.toDateTime(timeNow), cs.timeNow);

        return node.managerNodeRecord;
    }

    //------------------------
    static class TestCase{
        public int sec;
        public int nodeIdx;
        public boolean shouldBeManager;

        public static TestCase of(int sec, int nodeIdx, boolean shouldBeManager) {
            val tc = new TestCase();
            tc.sec = sec;
            tc.nodeIdx = nodeIdx;
            tc.shouldBeManager = shouldBeManager;
            return tc;
        }
    }

    //-----------------
    private int id = 0;
    private String nextId(){
        id++;
        return "ID_"+id;
    }
}