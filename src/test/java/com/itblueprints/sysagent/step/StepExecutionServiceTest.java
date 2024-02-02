package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.ExecStatus;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.ClusterInfo;
import com.itblueprints.sysagent.job.JobExecutionService;
import com.itblueprints.sysagent.repository.RecordRepository;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
class StepExecutionServiceTest {

    @Mock RecordRepository repository;
    @Mock
    JobExecutionService jobExecutionService;
    @Mock Config config;
    @Mock ThreadManager threadManager;

    StepExecutionService stepExecutionService;

    private final String jobName = "Job";
    private final String stepName = "Step";
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        when(threadManager.getBatchPageSize()).thenReturn(4);
        when(threadManager.getExecutor()).thenReturn(executor);
        when(threadManager.getTaskQueueSize()).thenReturn(2);

        stepExecutionService = new StepExecutionService(jobExecutionService, threadManager, repository);
    }

    //-------------------------------------
    @Test
    void tryProcessStep() {
        val clusterInfo = new ClusterInfo();

        val stepRec = createStepRecord();
        when(repository.tryClaimNextStepRecord(any())).thenReturn(stepRec);

        val step = new MockStep();
        when(jobExecutionService.getStep(jobName, stepName)).thenReturn(step);

        val now = LocalDateTime.of(2024, 1, 10, 0,0,0);
        val stepProcessed = stepExecutionService.processStepIfAvailable(clusterInfo, now);

        assertTrue(stepProcessed);
        assertEquals(ExecStatus.COMPLETE, stepRec.getStatus());
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