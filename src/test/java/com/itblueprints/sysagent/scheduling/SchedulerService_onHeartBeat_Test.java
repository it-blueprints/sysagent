package com.itblueprints.sysagent.scheduling;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.ClusterService;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.cluster.NodeState;
import com.itblueprints.sysagent.job.Job;
import com.itblueprints.sysagent.job.JobPipeline;
import com.itblueprints.sysagent.job.JobService;
import com.itblueprints.sysagent.step.StepService;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.support.CronExpression;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SchedulerService_onHeartBeat_Test {

    @Mock ConfigurableApplicationContext appContext;
    @Mock ThreadManager threadManager;
    @Mock MongoTemplate mongoTemplate;
    @Mock JobService jobService;
    @Mock Config config;

    SchedulerService schedulerService;

    private final String jobName = "com.itblueprints.payments.ProcessPaymentsJob";
    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        schedulerService = new SchedulerService(appContext, threadManager, mongoTemplate, jobService, config);
        when(config.getHeartBeatSecs()).thenReturn(10);

        val sji = new SchedulerService.ScheduledJobItem();
        sji.jobName = jobName;
        sji.cronExp = CronExpression.parse("0 0 0 * * *");
        schedulerService.scheduledJobItems.add(sji);

        val jsi = new JobScheduleRecord();
        jsi.setJobName(jobName);
        when(mongoTemplate.findById(jobName, JobScheduleRecord.class)).thenReturn(jsi);

    }

    //-------------------------------------
    @Test
    void onHeartBeat() {

        val jsi = new JobScheduleRecord();
        jsi.setJobName(jobName);
        when(mongoTemplate.findById(jobName, JobScheduleRecord.class)).thenReturn(jsi);

        var totalInvocations = 0;

        //1 hr before schedule
        val nodeInfo = new NodeInfo();
        val now1 = LocalDateTime.of(2024, 1, 10, 23,0,0);
        schedulerService.onHeartBeat(nodeInfo, now1);
        verify(threadManager, times(totalInvocations)).submit(any());

        //20 secs before schedule
        val now2 = LocalDateTime.of(2024, 1, 10, 23,59,40);
        schedulerService.onHeartBeat(nodeInfo, now2);
        totalInvocations++;
        verify(threadManager, times(totalInvocations)).submit(any());
        jsi.setLastRunAt(LocalDateTime.of(2024, 1, 11, 0,0,0));

        //Next day 31 secs before schedule
        val now3 = LocalDateTime.of(2024, 1, 11, 23,59,29);
        schedulerService.onHeartBeat(nodeInfo, now3);
        verify(threadManager, times(totalInvocations)).submit(any());

        //Next day 9 secs after schedule
        val now4 = LocalDateTime.of(2024, 1, 11, 0,0,9);
        schedulerService.onHeartBeat(nodeInfo, now4);
        totalInvocations++;
        verify(threadManager, times(totalInvocations)).submit(any());
        jsi.setLastRunAt(LocalDateTime.of(2024, 1, 12, 0,0,0));

        //19 secs after schedule
        val now5 = LocalDateTime.of(2024, 1, 12, 0,0,19);
        schedulerService.onHeartBeat(nodeInfo, now5);
        verify(threadManager, times(totalInvocations)).submit(any());
    }
}