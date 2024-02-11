package com.itblueprints.sysagent;

import com.itblueprints.sysagent.job.JobArguments;
import com.itblueprints.sysagent.internal.job.JobExecutionService;
import com.itblueprints.sysagent.internal.repository.RecordRepository;
import lombok.RequiredArgsConstructor;
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
        runJob(jobName, new JobArguments());
    }

    //--------------------------------
    public void runJob(String jobName, JobArguments jobArguments){
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
