package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.SystemAgentException;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.step.Step;
import com.itblueprints.sysagent.step.StepRecord;
import com.itblueprints.sysagent.scheduling.SchedulerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobService {

    private final ConfigurableApplicationContext appContext;
    private final MongoTemplate mongoTemplate;


    //--------------------------------------------------------------
    private Map<String, JobItem> jobsMap = new HashMap<>();

    public Step getStep(String jobName, String stepName){
        if(jobsMap.containsKey(jobName)) {
            val jobItem = jobsMap.get(jobName);
            if(jobItem.stepsMap.containsKey(stepName)) {
                val step = jobItem.stepsMap.get(stepName);
                return step;
            }
            else throw new SystemAgentException("Step "+stepName+" not found for Job "+jobName);
        }
        else throw new SystemAgentException("Job "+jobName+" not found");
    }


    //--------------------------------------------------------------
    public void runJob(String jobName, Arguments jobArgs, NodeInfo nodeInfo) {

        log.debug("Running "+jobName);

        val item = jobsMap.get(jobName);
        val runAt = jobArgs.asTime(SchedulerService.runAt);

        var jobRecord = new JobRecord();
        jobRecord.setJobName(jobName);
        jobRecord.setRunAt(runAt);
        jobRecord = mongoTemplate.save(jobRecord);

        val pipeline = item.pipeline;
        item.job.addToJobArguments(jobArgs);
        val firstStep = pipeline.getSteps().getFirst();
        val step = firstStep.step;

        val partArgs = step.getPartitionArguments(jobArgs);

        int totalPartitions = 1;
        if(partArgs!=null && !partArgs.isEmpty()){
            if(partArgs.size() < 2) throw new SystemAgentException("Minimum partitions is 2");
            totalPartitions = partArgs.size();
        }

        for(int i=0; i < totalPartitions; i++){
            val stepRecord = new StepRecord();
            stepRecord.setJobRecordId(jobRecord.getId());
            stepRecord.setJobName(jobName);
            stepRecord.setStepName(step.getName());
            stepRecord.setJobArguments(jobArgs);
            if(totalPartitions > 1) {
                val partArg = partArgs.get(i);
                stepRecord.setPartitionArguments(partArg);
                stepRecord.setPartitionNum(i);
                stepRecord.setTotalPartitions(totalPartitions);
            }
            mongoTemplate.save(stepRecord);
        }

    }

    //-----------------------------------------------
    public void initialise(NodeInfo nodeInfo){

        val beanFactory = appContext.getBeanFactory();
        val jobBeanNames = beanFactory.getBeanNamesForType(Job.class);
        for (val beanName : jobBeanNames) {
            val jobBean = beanFactory.getBean(beanName, Job.class);
            val item = new JobItem();
            item.job = jobBean;
            item.pipeline = jobBean.getPipeline();
            for(val ps : item.pipeline.getSteps()){
                item.stepsMap.put(ps.step.getName(), ps.step);
            }
            jobsMap.put(jobBean.getName(), item);
        }

        log.debug("JobService inited");
    }

    //-----------------------------------------
    public void onHeartBeat(NodeInfo nodeInfo) {
    }
}
