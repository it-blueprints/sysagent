package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.step.MockStep;
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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexOperations;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobServiceTest {

    @Mock ConfigurableApplicationContext appContext;
    @Mock MongoTemplate mongoTemplate;
    @Mock ThreadManager threadManager;
    @Mock ConfigurableListableBeanFactory beanFactory;
    @Mock IndexOperations indexOperations;

    @Captor ArgumentCaptor<StepRecord> stepRecC;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private JobService jobService;

    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        jobService = new JobService(appContext, mongoTemplate, threadManager);
    }

    //------------------------------------
    @Test
    void runJob() {

        val job = new MockJob();
        val jobRec = new JobRecord();
        jobRec.setJobName("Job");

        when(mongoTemplate.save(any())).thenReturn(jobRec);

        initialiseJobService(job, jobRec);

        val jobArgs = new Arguments();
        jobArgs.put("pmtProfile", "sp");
        jobService.runJob("Job", jobArgs);

        assertEquals("Step", jobRec.getCurrentStepName());
        assertEquals(3, jobRec.getPartitionCount());

        verify(mongoTemplate, times(3)).save(stepRecC.capture());

        val stepRecs = stepRecC.getAllValues();
        val n = stepRecs.stream()
                .filter(sr -> sr.getJobName().equals("Job")
                && sr.getJobRecordId().equals("job1234")
                && sr.getStepName().equals("Step")
                && sr.getJobArguments().asString("pmtProfile").equals("sp"))
                .count();
        assertEquals(3, n);

        val partnArgs = stepRecs.stream()
                .map(sr -> sr.getPartitionArguments().asInt("partition")).
                collect(Collectors.toList());
        assertEquals(List.of(1,2,3), partnArgs);
    }

    //------------------------------------
    @Test
    void onHeartBeat() {
        val job = new MockJob();
        val jobRec = new JobRecord();
        jobRec.setJobName("Job");
        jobRec.setCurrentStepName("Step");

        when(mongoTemplate.find(any(), eq(JobRecord.class))).thenReturn(List.of(jobRec));
        when(threadManager.getExecutor()).thenReturn(executor);
        initialiseJobService(job, jobRec);
        jobService.onHeartBeat(new NodeInfo(), LocalDateTime.now());
    }

    //-----------------------------------------------------------
    private void initialiseJobService(Job job, JobRecord jobRec){

        jobRec.setJobName("Job");
        jobRec.setId("job1234");
        jobRec.setStatus(JobRecord.Status.Executing);

        when(appContext.getBeanFactory()).thenReturn(beanFactory);
        when(beanFactory.getBeanNamesForType(Job.class)).thenReturn(new String[]{"job"});
        when(beanFactory.getBean("job", Job.class)).thenReturn(job);
        when(mongoTemplate.indexOps(JobRecord.class)).thenReturn(indexOperations);
        jobService.initialise(new NodeInfo());

    }

    //-----------------------------------
    static class MockJob implements Job {

        @Override
        public JobPipeline getPipeline() {
            return JobPipeline.create()
                    .firstStep(new MockStep());
        }

        @Override
        public void addToJobArguments(Arguments jobArguments) {

        }

        @Override
        public String getName() {
            return "Job";
        }
    }

}