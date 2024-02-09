package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.ExecutionStatus;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.job.JobArguments;
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
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class StepExecutionServiceTest {

    @Mock RecordRepository repository;
    @Mock JobExecutionService jobExecutionService;
    @Mock Config config;
    @Mock ThreadManager threadManager;

    StepExecutionService stepExecutionService;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private LocalDateTime now = LocalDateTime.now();

    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        stepExecutionService = new StepExecutionService(jobExecutionService, threadManager, repository);
    }

    //-------------------------------------
    @Test
    void processStepIfAvailable() {
        val clusterInfo = new NodeInfo();
        val stepRec = createStepRecord();
        when(repository.tryClaimNextStepRecord(any())).thenReturn(stepRec);

        val step = new MockPartitionedStep();
        when(jobExecutionService.getStep("Job", "Step")).thenReturn(step);

        val stepProcessed = stepExecutionService.processStepIfAvailable(clusterInfo, now);

        assertTrue(stepProcessed);
    }

    //-------------------------------------
    @Test
    void processStep() {
        val stepRec = createStepRecord();
        val prtn = new Partition();
        prtn.setPartitionCount(3);
        prtn.setPartitionNum(1);
        stepRec.setPartition(prtn);
        val step = new MockPartitionedStep();
        when(jobExecutionService.getStep("Job", "Step")).thenReturn(step);

        stepExecutionService.processStep(stepRec, now);

        assertEquals(ExecutionStatus.COMPLETE, stepRec.getStatus());
        assertEquals(now, stepRec.getStartedAt());
        verify(threadManager, times(1)).setNodeBusy(true);
        verify(threadManager, times(1)).setNodeBusy(false);
        verify(repository,times(2)).save(stepRec);
        assertTrue(step.runCalled);
        val ctx = step.stepContext;
        assertEquals(stepRec.getPartition().getPartitionCount(), ctx.getPartition().getPartitionCount());
        assertEquals(stepRec.getPartition().getPartitionNum(), ctx.getPartition().getPartitionNum());
    }

    //------------------------------------
    @Test
    void runBatched() {
        when(threadManager.getBatchPageSize()).thenReturn(4);
        when(threadManager.getExecutor()).thenReturn(executor);
        when(threadManager.getTaskQueueSize()).thenReturn(2);

        val step = new MockBatchPartitionedStep();
        val ctx = new BatchStepContext();
        stepExecutionService.runBatched(step, ctx);

        assertEquals(11, ctx.getItemsProcessed());
        assertEquals(3, step.totalPages);
        assertEquals(true, step.onStartCalled);
        assertEquals(true, step.onCompleteCalled);
        assertEquals(3, step.readPageOfItems_TimesCalled);
        assertEquals(11, step.processItem_TimesCalled);
        assertEquals(3, step.writePageOfItems_TimesCalled);
        assertEquals(List.of("A_X", "B_X", "C_X", "D_X", "E_X", "F_X", "G_X", "H_X", "I_X", "J_X", "K_X"), step.result);
    }

    //------------------------------------
    private StepRecord createStepRecord(){
        val stepRec = new StepRecord();
        stepRec.setJobName("Job");
        stepRec.setStepName("Step");
        stepRec.setClaimed(true);
        val jobArgs = new JobArguments();
        jobArgs.put("procDate", LocalDate.of(2024, 1, 12));
        jobArgs.put("pmtProfile", "state_pension");
        stepRec.setJobArguments(jobArgs);
        val prtn = new Partition();
        prtn.put("custProfile", "resident_GB");
        prtn.put("partition", 1);
        prtn.setPartitionNum(0);
        prtn.setPartitionCount(10);
        stepRec.setPartition(prtn);
        return stepRec;
    }


}