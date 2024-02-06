package com.itblueprints.sysagent;

import com.itblueprints.sysagent.cluster.NodeRecord;
import com.itblueprints.sysagent.job.JobRecord;
import com.itblueprints.sysagent.job.JobExecutionService;
import com.itblueprints.sysagent.scheduling.JobScheduleRecord;
import com.itblueprints.sysagent.step.StepRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class SysAgentService {

    private final MongoOperations mongoOperations;
    private final JobExecutionService jobExecutionService;

    //--------------------------------
    public void resetCluster() {
        mongoOperations.dropCollection(JobRecord.class);
        mongoOperations.dropCollection(JobScheduleRecord.class);
        mongoOperations.dropCollection(NodeRecord.class);
        mongoOperations.dropCollection(StepRecord.class);
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
