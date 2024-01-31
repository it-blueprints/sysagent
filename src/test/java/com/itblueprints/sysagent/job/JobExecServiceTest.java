package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.ExecStatus;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.ClusterInfo;
import com.itblueprints.sysagent.repository.RecordRepository;
import com.itblueprints.sysagent.step.StepRecord;
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
import org.springframework.data.mongodb.core.index.IndexOperations;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobExecServiceTest {

    @Mock ConfigurableApplicationContext appContext;
    @Mock RecordRepository repository;
    @Mock ThreadManager threadManager;
    @Mock ConfigurableListableBeanFactory beanFactory;
    @Mock IndexOperations indexOperations;

    @Captor ArgumentCaptor<StepRecord> stepRecC;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private JobExecService jobExecService;

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
        assertEquals(4, jobRec.getPartitionCount());

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
        val jobRec = JobRecord.create("Job", new Arguments(), LocalDateTime.now());
        jobRec.setId("1");
        jobRec.setCurrentStepName("Step");

        val stepRec = StepRecord.create("1","Job", "Step", new Arguments());

        when(repository.findRecordsForRunningJobs()).thenReturn(List.of(jobRec));
        lenient().when(repository.getRecordsOfStepOfJob(any(), any())).thenReturn(List.of(stepRec));
        when(threadManager.getExecutor()).thenReturn(executor);
        initialiseJobService(job, jobRec);
        jobExecService.processExecutingJobs(LocalDateTime.now());

        //TODO
    }

    //------------------------------------
    @Test
    void releaseDeadClaims() {
        //TODO
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

}