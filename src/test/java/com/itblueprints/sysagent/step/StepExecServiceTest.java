package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.job.JobExecService;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StepExecServiceTest {

    @Mock MongoTemplate mongoTemplate;
    @Mock
    JobExecService jobExecService;
    @Mock Config config;
    @Mock ThreadManager threadManager;

    StepExecService stepExecService;

    private final String jobName = "Job";
    private final String stepName = "Step";
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        when(threadManager.getBatchPageSize()).thenReturn(4);
        when(threadManager.getExecutor()).thenReturn(executor);
        when(threadManager.getWorkerTaskQueuSize()).thenReturn(4);

        stepExecService = new StepExecService(mongoTemplate, jobExecService, threadManager);
    }

    //-------------------------------------
    @Test
    void tryProcessStep() {
        val nodeInfo = new NodeInfo();

        val stepRec = createStepRecord();
        when(mongoTemplate.findAndModify(any(), any(), any())).thenReturn(stepRec);

        val step = new MockStep();
        when(jobExecService.getStep(jobName, stepName)).thenReturn(step);

        val now = LocalDateTime.of(2024, 1, 10, 0,0,0);
        val stepProcessed = stepExecService.tryProcessStep(nodeInfo, now);

        assertTrue(stepProcessed);
        assertEquals(StepRecord.Status.Completed, stepRec.getStatus());
        assertEquals(3, step.totalPages);
        assertEquals(true, step.preProcessCalled);
        assertEquals(true, step.postProcessCalled);
        assertEquals(3, step.readChunkOfItems_TimesCalled);
        assertEquals(11, step.processItem_TimesCalled);
        assertEquals(List.of("A_X", "B_X", "C_X", "D_X", "E_X", "F_X", "G_X", "H_X", "I_X", "J_X", "K_X"), step.result);
    }

    //------------------------------------
    private StepRecord createStepRecord(){
        val stepRec = new StepRecord();
        stepRec.setJobName(jobName);
        stepRec.setStepName(stepName);
        stepRec.setClaimed(true);
        val jobArgs = new Arguments();
        jobArgs.put("procDate", LocalDate.of(2024, 1, 12));
        jobArgs.put("pmtProfile", "state_pension");
        stepRec.setJobArguments(jobArgs);
        val prtArgs = new Arguments();
        prtArgs.put("custProfile", "resident_GB");
        prtArgs.put("partition", 1);
        stepRec.setPartitionArguments(prtArgs);
        stepRec.setPartitionNum(0);
        stepRec.setPartitionCount(10);
        return stepRec;
    }


}