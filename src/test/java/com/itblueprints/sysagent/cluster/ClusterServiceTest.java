package com.itblueprints.sysagent.cluster;

import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.TestUtils;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.job.JobExecutionService;
import com.itblueprints.sysagent.scheduling.SchedulerService;
import com.itblueprints.sysagent.step.StepExecutionService;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


@ExtendWith(MockitoExtension.class)
class ClusterServiceTest {

    @Mock SchedulerService schedulerService;
    @Mock
    JobExecutionService jobExecutionService;
    @Mock
    StepExecutionService stepExecutionService;
    @Mock Config config;
    @Mock ThreadManager threadManager;

    List<ClusterService> nodes;

    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        val repository = TestUtils.getRecordRepository(this.getClass());
        nodes = List.of(
                new ClusterService(repository, schedulerService, jobExecutionService, stepExecutionService, config, threadManager),
                new ClusterService(repository, schedulerService, jobExecutionService, stepExecutionService, config, threadManager),
                new ClusterService(repository, schedulerService, jobExecutionService, stepExecutionService, config, threadManager),
                new ClusterService(repository, schedulerService, jobExecutionService, stepExecutionService, config, threadManager)
        );
    }

    private static final int HB_SECS = 10;
    private static final long START_TIME = 1700000000000L;

    private ManagerNodeRecord mgrNodeRecord;
    private long toTime(int sec){
        return START_TIME + sec * 1000;
    }
    //-------------------------------------
    @Test
    void computeClusterState() {

        val testCases = List.of(
                TestCase.of(0, 0, true), //start, manager = 0
                TestCase.of(1, 1, false),
                TestCase.of(10, 0, true),
                TestCase.of(11, 1, false),
                TestCase.of(31, 1, true), //manager change 0 -> 1
                TestCase.of(31, 2, false),
                TestCase.of(32, 0, false),
                TestCase.of(41, 2, false),
                TestCase.of(51, 2, false),
                TestCase.of(52, 3, true), //manager change 1 -> 3
                TestCase.of(61, 2, false)
        );

        val i = new int[]{1};
        testCases.stream().forEach( tc -> {
                    mgrNodeRecord = assertState(tc, i[0]);
                    i[0]++;
                });

        val currentMgr = nodes.get(3);
        val currentWorker = nodes.get(2);
        val currentDead = nodes.get(1);

        assertEquals(toTime(72), currentMgr.nodeRecord.getLifeLeaseTill());
        assertEquals(toTime(72), currentMgr.managerNodeRecord.getManagerLeaseTill());
        assertEquals(toTime(81), currentWorker.nodeRecord.getLifeLeaseTill());
        assertEquals(toTime(51), currentDead.nodeRecord.getLifeLeaseTill());
    }
    //-------------------------------------------------
    private ManagerNodeRecord assertState(TestCase testCase, int testNum){

        val timeNow = START_TIME + testCase.sec * 1000;

        val nIdx = testCase.nodeIdx;
        val node = nodes.get(nIdx);

        val cs = node.computeNodeInfo(HB_SECS, timeNow);

        //cluster state
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