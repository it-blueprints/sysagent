package com.itblueprints.sysagent.internal.job;

import com.itblueprints.sysagent.internal.ExecutionStatus;
import com.itblueprints.sysagent.internal.SysAgentException;
import com.itblueprints.sysagent.internal.ThreadManager;
import com.itblueprints.sysagent.internal.cluster.NodeInfo;
import com.itblueprints.sysagent.internal.repository.RecordRepository;
import com.itblueprints.sysagent.internal.step.StepRecord;
import com.itblueprints.sysagent.job.Job;
import com.itblueprints.sysagent.job.JobArguments;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.util.Pair;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.itblueprints.sysagent.TestUtils.assertTrueForAll;
import static com.mongodb.assertions.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobExecutionServiceTest {

    @Mock ConfigurableApplicationContext appContext;
    @Mock RecordRepository repository;
    @Mock ThreadManager threadManager;
    @Mock ConfigurableListableBeanFactory beanFactory;

    @Captor ArgumentCaptor<StepRecord> stepRecC;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private JobExecutionService jobExecutionService;
    private LocalDateTime now = LocalDateTime.now();

    //-------------------------------------
    @BeforeEach
    void beforeEach() {

        jobExecutionService = new JobExecutionService(appContext, repository, threadManager);
        loadMockJobIntoService();
    }

    //------------------------------------
    @Test
    void runJob() {

        val jobRec = JobRecord.of("Job", new JobArguments(), now);
        jobRec.setId("jrid1");
        jobRec.setStatus(ExecutionStatus.RUNNING);

        when(repository.save((JobRecord) any())).thenReturn(jobRec);

        val jobArgs = jobRec.getJobArguments();
        jobArgs.put("pmtProfile", "sp");
        jobExecutionService.runJob("Job", jobArgs);

        assertEquals("Step1", jobRec.getCurrentStepName());
        assertEquals(4, jobRec.getCurrentStepPartitionCount());

        verify(repository, times(4)).save(stepRecC.capture());

        val stepRecs = stepRecC.getAllValues();

        assertTrueForAll(stepRecs, sr -> sr.getJobName().equals("Job"));
        assertTrueForAll(stepRecs, sr -> sr.getJobRecordId().equals("jrid1"));
        assertTrueForAll(stepRecs, sr -> sr.getStepName().equals("Step1"));
        assertTrueForAll(stepRecs, sr -> sr.getJobArguments().getString("pmtProfile").equals("sp"));

        val partnArgs = stepRecs.stream()
                .map(sr -> {
                    val sb = new StringBuilder();
                    sb.append("k1:").append(sr.getPartition().getString("k1")).append("+");
                    sb.append("k2:").append(sr.getPartition().getString("k2"));
                    return sb.toString();
                }).
                collect(Collectors.toList());
        assertEquals(List.of("k1:k1v1+k2:k2v1", "k1:k1v2+k2:k2v1", "k1:k1v3+k2:k2v2", "k1:k1v4+k2:k2v2"), partnArgs);
    }


    //------------------------------------
    @Test
    void processExecutingJobs() {
        val jobRec = JobRecord.of("Job", new JobArguments(), now);
        jobRec.setId("jrid1");
        jobRec.setStatus(ExecutionStatus.RUNNING);
        jobRec.setCurrentStepName("Step");

        val stepRec = StepRecord.of("1","Job", "Step", new JobArguments());

        when(repository.getRunningJobRecords()).thenReturn(List.of(jobRec));
        lenient().when(repository.getStepsRecordsForStepOfJob(any(), any())).thenReturn(List.of(stepRec));
        when(threadManager.getExecutor()).thenReturn(executor);

        jobExecutionService.processExecutingJobs(now);

        //TODO
    }

    //------------------------------------
    @Test
    void isCurrentStepComplete() {

        //***** Test partitioned step *******
        val testData = createTestJobAndStepRecords();
        val jobRec = testData.getFirst();
        val stepRecs = testData.getSecond();

        //2 steps ccomplete, one not yet
        stepRecs.get(0).setStatus(ExecutionStatus.COMPLETE);
        stepRecs.get(1).setStatus(ExecutionStatus.COMPLETE);
        val result1 = jobExecutionService.isCurrentStepComplete(jobRec, stepRecs);
        assertFalse(result1);

        //all steps now complete
        stepRecs.get(2).setStatus(ExecutionStatus.COMPLETE);
        val result2 = jobExecutionService.isCurrentStepComplete(jobRec, stepRecs);
        assertTrue(result2);

        //***** Test single step *****
        jobRec.setCurrentStepPartitionCount(0);
        val onlyStepRec = List.of(stepRecs.get(0));
        onlyStepRec.get(0).setStatus(ExecutionStatus.COMPLETE);
        val result3 = jobExecutionService.isCurrentStepComplete(jobRec, onlyStepRec);
        assertTrue(result3);

        //***** Test with empty step recs ****
        jobRec.setCurrentStepPartitionCount(2);
        List<StepRecord> noStepRecs = List.of();
        assertThrows(SysAgentException.class, () -> {
            jobExecutionService.isCurrentStepComplete(jobRec, noStepRecs);
        });
    }

    //------------------------------------
    @Test
    void hasCurrentStepFailed() {

        //***** Test partitioned step *******
        val testData = createTestJobAndStepRecords();
        val jobRec = testData.getFirst();
        val stepRecs = testData.getSecond();

        //2 steps ccomplete, 1 failed
        stepRecs.get(0).setStatus(ExecutionStatus.COMPLETE);
        stepRecs.get(1).setStatus(ExecutionStatus.COMPLETE);
        stepRecs.get(2).setStatus(ExecutionStatus.FAILED);
        val result1 = jobExecutionService.hasCurrentStepFailed(jobRec, stepRecs);
        assertTrue(result1);

        //***** Test single step *****
        jobRec.setCurrentStepPartitionCount(0);
        val onlyStepRec = List.of(stepRecs.get(0));
        onlyStepRec.get(0).setStatus(ExecutionStatus.FAILED);
        val result2 = jobExecutionService.hasCurrentStepFailed(jobRec, onlyStepRec);
        assertTrue(result2);

        //***** Test with empty step recs ****
        jobRec.setCurrentStepPartitionCount(2);
        List<StepRecord> noStepRecs = List.of();
        assertThrows(SysAgentException.class, () -> {
            jobExecutionService.hasCurrentStepFailed(jobRec, noStepRecs);
        });

    }


    //------------------------------------
    @Test
    void sendStepExecutionInstruction() {

        val testData = createTestJobAndStepRecords();
        val jobRec = testData.getFirst();
        val stepRecs = testData.getSecond();

        val step = new MockStep1();

        jobExecutionService.sendStepExecutionInstruction(step, new JobArguments(), jobRec);

        assertEquals(4, jobRec.getCurrentStepPartitionCount());
        verify(repository, times(4)).save(stepRecC.capture());

        val savedStepRecs = stepRecC.getAllValues();
        assertEquals("{k1=k1v1, k2=k2v1}", savedStepRecs.get(0).getPartition().toString());
        assertEquals("{k1=k1v2, k2=k2v1}", savedStepRecs.get(1).getPartition().toString());
        assertEquals("{k1=k1v3, k2=k2v2}", savedStepRecs.get(2).getPartition().toString());
        assertEquals("{k1=k1v4, k2=k2v2}", savedStepRecs.get(3).getPartition().toString());


        assertTrueForAll(savedStepRecs, sr -> sr.getJobRecordId().equals("jrid1"));
        assertTrueForAll(savedStepRecs, sr -> sr.getJobName().equals("Job"));
        assertTrueForAll(savedStepRecs, sr -> sr.getStepName().equals("Step1"));
        assertTrueForAll(savedStepRecs, sr -> sr.getStatus() == ExecutionStatus.NEW);
    }



    //------------------------------------
    @Test
    void releaseDeadClaims() {
        val clInfo = new NodeInfo();
        clInfo.deadNodeIds = List.of("node1_id", "node2_id");

        val sr1 = StepRecord.of("jobrecid1", "Job", "Step1", new JobArguments());
        sr1.setClaimed(true);
        sr1.setNodeId("node1_id");
        when(repository.getStepRecordsClaimedByNode("node1_id")).thenReturn(List.of(sr1));

        val sr2 = StepRecord.of("jobrecid1", "Job", "Step2", new JobArguments());
        sr2.setClaimed(true);
        sr2.setNodeId("node2_id");
        when(repository.getStepRecordsClaimedByNode("node2_id")).thenReturn(List.of(sr2));

        jobExecutionService.releaseDeadClaims(clInfo);

        verify(repository, times(2)).save((StepRecord) any());

        val stepRecs = List.of(sr1, sr2);
        assertTrueForAll(stepRecs, sr -> sr.isClaimed() == false);
        assertTrueForAll(stepRecs, sr -> sr.getNodeId() == null);

    }

    //------------------------------------
    @Test
    void retryFailedJob() {
        val testData = createTestJobAndStepRecords();
        val jobRec = testData.getFirst();
        val stepRecs = testData.getSecond().subList(0,1);
        when(repository.getFailedJobRecordOfJob("Job")).thenReturn(jobRec);
        when(repository.getFailedStepRecordsForJob("jrid1")).thenReturn(stepRecs);

        jobExecutionService.retryFailedJob("Job", now);

        verify(repository, times(1)).save((StepRecord) any());

        val sr = stepRecs.get(0);
        assertEquals(ExecutionStatus.NEW, sr.getStatus());
        assertEquals(false, sr.isClaimed());
        assertEquals(1, sr.getRetryCount());
        assertEquals(now, sr.getLastUpdateAt());
    }

    //-----------------------------------------------------------
    private void loadMockJobIntoService(){
        val job = new MockJob();
        when(appContext.getBeanFactory()).thenReturn(beanFactory);
        when(beanFactory.getBeanNamesForType(Job.class)).thenReturn(new String[]{"job"});
        when(beanFactory.getBean("job", Job.class)).thenReturn(job);
        jobExecutionService.initialise(new NodeInfo());

    }

    //----------------------------------------------------------------
    private Pair<JobRecord, List<StepRecord>> createTestJobAndStepRecords(){
        val jobRec = JobRecord.of("Job", new JobArguments(), now);
        jobRec.setId("jrid1");
        jobRec.setCurrentStepName("Step1");
        jobRec.setCurrentStepPartitionCount(3);
        val stepRec1 =StepRecord.of("jrid1", "Job", "Step1", new JobArguments());
        val stepRec2 =StepRecord.of("jrid1", "Job", "Step1", new JobArguments());
        val stepRec3 =StepRecord.of("jrid1", "Job", "Step1", new JobArguments());
        return Pair.of(jobRec, List.of(stepRec1, stepRec2, stepRec3));
    }

}