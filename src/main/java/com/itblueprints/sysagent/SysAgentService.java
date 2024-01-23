package com.itblueprints.sysagent;

import com.itblueprints.sysagent.cluster.ClusterService;
import com.itblueprints.sysagent.cluster.NodeState;
import com.itblueprints.sysagent.job.JobRecord;
import com.itblueprints.sysagent.job.JobService;
import com.itblueprints.sysagent.scheduling.JobScheduleRecord;
import com.itblueprints.sysagent.step.StepRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SysAgentService {

    private final MongoTemplate mongoTemplate;
    private final JobService jobService;

    //--------------------------------
    public void deleteClusterData() {
        mongoTemplate.remove(new Query(), JobRecord.class);
        mongoTemplate.remove(new Query(), JobScheduleRecord.class);
        mongoTemplate.remove(new Query(), NodeState.class);
        mongoTemplate.remove(new Query(), StepRecord.class);
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
