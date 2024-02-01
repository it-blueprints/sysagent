package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.ExecStatus;
import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.ClusterInfo;
import com.itblueprints.sysagent.repository.RecordRepository;
import com.itblueprints.sysagent.step.StepRecord;
import com.mongodb.Function;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.itblueprints.sysagent.TestUtils.assertTrueForAll;
import static com.mongodb.assertions.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobExecServiceTest {

    @Mock ConfigurableApplicationContext appContext;
    @Mock RecordRepository repository;
    @Mock ThreadManager threadManager;
    @Mock ConfigurableListableBeanFactory beanFactory;

    @Captor ArgumentCaptor<StepRecord> stepRecC;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private JobExecService jobExecService;
    private LocalDateTime now = LocalDateTime.now();

    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        jobExecService = new JobExecService(appContext, repository, threadManager);
    }

    //------------------------------------
    @Test
    void runJob() {

        val job = new MockJob();
        val jobRec = new JobRecord();
        jobRec.setJobName("Job");

        when(repository.save((JobRecord) any())).thenReturn(jobRec);

        initialiseJobService(job, jobRec);

        val jobArgs = new Arguments();
        jobArgs.put("pmtProfile", "sp");
        jobExecService.runJob("Job", jobArgs);

        assertEquals("Step1", jobRec.getCurrentStepName());
        assertEquals(4, jobRec.getCurrentStepPartitionCount());

        verify(repository, times(4)).save(stepRecC.capture());

        val stepRecs = stepRecC.getAllValues();
        val n = stepRecs.stream()
                .filter(sr -> sr.getJobName().equals("Job")
                && sr.getJobRecordId().equals("job1234")
                && sr.getStepName().equals("Step1")
                && sr.getJobArguments().asString("pmtProfile").equals("sp"))
                .count();
        assertEquals(4, n);

        val partnArgs = stepRecs.stream()
                .map(sr -> {
                    val sb = new StringBuilder();
                    sb.append("k1:").append(sr.getPartitionArguments().asString("k1")).append("+");
                    sb.append("k2:").append(sr.getPartitionArguments().asString("k2"));
                    return sb.toString();
                }).
                collect(Collectors.toList());
        assertEquals(List.of("k1:k1v1+k2:k2v1", "k1:k1v2+k2:k2v1", "k1:k1v3+k2:k2v2", "k1:k1v4+k2:k2v2"), partnArgs);
    }


    //------------------------------------
    @Test
    void processExecutingJobs() {
        val job = new MockJob();
        val jobRec = JobRecord.of("Job", new Arguments(), now);
        jobRec.setId("1");
        jobRec.setCurrentStepName("Step");

        val stepRec = StepRecord.of("1","Job", "Step", new Arguments());

        when(repository.findRecordsForRunningJobs()).thenReturn(List.of(jobRec));
        lenient().when(repository.getRecordsOfStepOfJob(any(), any())).thenReturn(List.of(stepRec));
        when(threadManager.getExecutor()).thenReturn(executor);
        initialiseJobService(job, jobRec);
        jobExecService.processExecutingJobs(now);

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
        stepRecs.get(0).setStatus(ExecStatus.COMPLETE);
        stepRecs.get(1).setStatus(ExecStatus.COMPLETE);
        val result1 = jobExecService.isCurrentStepComplete(jobRec, stepRecs);
        assertFalse(result1);

        //all steps now complete
        stepRecs.get(2).setStatus(ExecStatus.COMPLETE);
        val result2 = jobExecService.isCurrentStepComplete(jobRec, stepRecs);
        assertTrue(result2);

        //***** Test single step *****
        jobRec.setCurrentStepPartitionCount(0);
        val onlyStepRec = List.of(stepRecs.get(0));
        onlyStepRec.get(0).setStatus(ExecStatus.COMPLETE);
        val result3 = jobExecService.isCurrentStepComplete(jobRec, onlyStepRec);
        assertTrue(result3);

        //***** Test with empty step recs ****
        jobRec.setCurrentStepPartitionCount(2);
        List<StepRecord> noStepRecs = List.of();
        assertThrows(SysAgentException.class, () -> {
            jobExecService.isCurrentStepComplete(jobRec, noStepRecs);
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
        stepRecs.get(0).setStatus(ExecStatus.COMPLETE);
        stepRecs.get(1).setStatus(ExecStatus.COMPLETE);
        stepRecs.get(2).setStatus(ExecStatus.FAILED);
        val result1 = jobExecService.hasCurrentStepFailed(jobRec, stepRecs);
        assertTrue(result1);

        //***** Test single step *****
        jobRec.setCurrentStepPartitionCount(0);
        val onlyStepRec = List.of(stepRecs.get(0));
        onlyStepRec.get(0).setStatus(ExecStatus.FAILED);
        val result2 = jobExecService.hasCurrentStepFailed(jobRec, onlyStepRec);
        assertTrue(result2);

        //***** Test with empty step recs ****
        jobRec.setCurrentStepPartitionCount(2);
        List<StepRecord> noStepRecs = List.of();
        assertThrows(SysAgentException.class, () -> {
            jobExecService.hasCurrentStepFailed(jobRec, noStepRecs);
        });

    }


    //------------------------------------
    @Test
    void sendStepExecutionInstruction() {

        val testData = createTestJobAndStepRecords();
        val jobRec = testData.getFirst();
        val stepRecs = testData.getSecond();

        val step = new MockStep1();

        jobExecService.sendStepExecutionInstruction(step, Arguments.of(), jobRec);

        assertEquals(4, jobRec.getCurrentStepPartitionCount());
        verify(repository, times(4)).save(stepRecC.capture());

        val savedStepRecs = stepRecC.getAllValues();
        assertEquals("{k1=k1v1, k2=k2v1}", savedStepRecs.get(0).getPartitionArguments().toString());
        assertEquals("{k1=k1v2, k2=k2v1}", savedStepRecs.get(1).getPartitionArguments().toString());
        assertEquals("{k1=k1v3, k2=k2v2}", savedStepRecs.get(2).getPartitionArguments().toString());
        assertEquals("{k1=k1v4, k2=k2v2}", savedStepRecs.get(3).getPartitionArguments().toString());


        assertTrueForAll(savedStepRecs, sr -> sr.getJobRecordId().equals("jrid1"));
        assertTrueForAll(savedStepRecs, sr -> sr.getJobName().equals("Job"));
        assertTrueForAll(savedStepRecs, sr -> sr.getStepName().equals("Step1"));
        assertTrueForAll(savedStepRecs, sr -> sr.getStatus() == ExecStatus.NEW);
    }



    //------------------------------------
    @Test
    void releaseDeadClaims() {
        //TODO
    }

    //------------------------------------
    @Test
    void retryFailedJob() {

    }

    @Test
    void initialise() {

    }

    //-----------------------------------------------------------
    private void initialiseJobService(Job job, JobRecord jobRec){

        jobRec.setJobName("Job");
        jobRec.setId("job1234");
        jobRec.setStatus(ExecStatus.RUNNING);

        when(appContext.getBeanFactory()).thenReturn(beanFactory);
        when(beanFactory.getBeanNamesForType(Job.class)).thenReturn(new String[]{"job"});
        when(beanFactory.getBean("job", Job.class)).thenReturn(job);
        jobExecService.initialise(new ClusterInfo());

    }

    //----------------------------------------------------------------
    private Pair<JobRecord, List<StepRecord>> createTestJobAndStepRecords(){
        val jobRec = JobRecord.of("Job", Arguments.of(), now);
        jobRec.setId("jrid1");
        jobRec.setCurrentStepName("Step1");
        jobRec.setCurrentStepPartitionCount(3);
        val stepRec1 =StepRecord.of("jrid1", "Job", "Step1", Arguments.of());
        val stepRec2 =StepRecord.of("jrid1", "Job", "Step1", Arguments.of());
        val stepRec3 =StepRecord.of("jrid1", "Job", "Step1", Arguments.of());
        return Pair.of(jobRec, List.of(stepRec1, stepRec2, stepRec3));
    }

}