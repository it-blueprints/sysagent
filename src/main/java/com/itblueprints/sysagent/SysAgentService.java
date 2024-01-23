package com.itblueprints.sysagent;

import com.itblueprints.sysagent.cluster.ClusterService;
import com.itblueprints.sysagent.job.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import javax.management.Query;

@Component
@RequiredArgsConstructor
public class SysAgentService {

    private final MongoTemplate mongoTemplate;
    private final JobService jobService;

    //--------------------------------
    public void deleteClusterData() {
        mongoTemplate.remove(new Query(), "jobRecord");
        mongoTemplate.remove(new Query(), "jobScheduleRecord");
        mongoTemplate.remove(new Query(), "nodeState");
        mongoTemplate.remove(new Query(), "stepRecord");
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
