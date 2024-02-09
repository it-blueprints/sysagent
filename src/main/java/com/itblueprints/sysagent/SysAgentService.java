package com.itblueprints.sysagent;

import com.itblueprints.sysagent.cluster.NodeRecord;
import com.itblueprints.sysagent.job.JobRecord;
import com.itblueprints.sysagent.job.JobExecutionService;
import com.itblueprints.sysagent.repository.RecordRepository;
import com.itblueprints.sysagent.scheduling.JobScheduleRecord;
import com.itblueprints.sysagent.step.StepRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class SysAgentService {

    private final RecordRepository repository;
    private final JobExecutionService jobExecutionService;

    //--------------------------------
    public void resetCluster() {
        repository.clearAll();
    }

    //--------------------------------
    public void runJob(String jobName){
        runJob(jobName, new Arguments());
    }

    //--------------------------------
    public void runJob(String jobName, Arguments jobArguments){
        jobExecutionService.runJob(jobName, jobArguments);
    }


    //------------------------------------------------
    public void retryFailedJob(String jobName){
        jobExecutionService.retryFailedJob(jobName, LocalDateTime.now());
    }

    //--------------------------------
    public static class DataKeys {
        public static final String jobStartedAt = "jobStartedAt";
    }
}
