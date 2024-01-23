package com.itblueprints.sysagent;

import com.itblueprints.sysagent.cluster.NodeState;
import com.itblueprints.sysagent.job.JobRecord;
import com.itblueprints.sysagent.job.JobService;
import com.itblueprints.sysagent.scheduling.JobScheduleRecord;
import com.itblueprints.sysagent.step.StepRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SysAgentService {

    private final MongoOperations mongoOperations;
    private final JobService jobService;

    //--------------------------------
    public void deleteClusterData() {
        mongoOperations.dropCollection(JobRecord.class);
        mongoOperations.dropCollection(JobScheduleRecord.class);
        mongoOperations.dropCollection(NodeState.class);
        mongoOperations.dropCollection(StepRecord.class);
    }

    //--------------------------------
    public void runJob(String jobName){
        runJob(jobName, new Arguments());
    }

    //--------------------------------
    public void runJob(String jobName, Arguments jobArguments){
        jobService.runJob(jobName, jobArguments);
    }
}
