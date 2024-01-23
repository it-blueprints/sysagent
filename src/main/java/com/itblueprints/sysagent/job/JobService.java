package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.step.Step;
import com.itblueprints.sysagent.step.StepRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
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
                val pStep = jobItem.stepsMap.get(stepName);
                return pStep.step;
            }
            else throw new SysAgentException("Step "+stepName+" not found for Job "+jobName);
        }
        else throw new SysAgentException("Job "+jobName+" not found");
    }


    //--------------------------------------------------------------
    public void runJob(String jobName, Arguments jobArgs) {

        log.debug("Running "+jobName);

        if(!jobArgs.contains(jobStartedAt)){
            jobArgs.put(jobStartedAt, LocalDateTime.now());
        }

        val jobStartedAt = jobArgs.asTime(JobService.jobStartedAt);

        val jRec = new JobRecord();
        jRec.setJobName(jobName);
        jRec.setJobStartedAt(jobStartedAt);
        jRec.setStatus(JobRecord.Status.Executing);
        var jobRecord = mongoTemplate.save(jRec);

        val jobItem = jobsMap.get(jobName);
        val firstStep = jobItem.firstStep;
        jobItem.job.addToJobArguments(jobArgs);

        sendStepExecutionInstruction(firstStep, jobArgs, jobRecord);

    }

    public void runJob(String jobName) {
        runJob(jobName, new Arguments());
    }

    //------------------------------------------------------------
    public void onHeartBeat(NodeInfo nodeInfo, LocalDateTime now) {
        val query = new Query();
        query.addCriteria(Criteria
                .where("status").is(JobRecord.Status.Executing));
        val executingJobs = mongoTemplate.find(query, JobRecord.class);
        for(val jobRec : executingJobs){
            val query2 = new Query();
            query2.addCriteria(Criteria
                    .where("jobRecordId").is(jobRec.getId())
                    .and("stepName").is(jobRec.getCurrentStepName())
                    .and("status").is(StepRecord.Status.Completed)
            );
            val completedPrtns = mongoTemplate.find(query2, StepRecord.class);

            //If all partitions are complete, job is complete
            if(completedPrtns.size() == jobRec.getCurrentStepPartitionCount()){

                val jobItem = jobsMap.get(jobRec.getJobName());
                val jobArgs = new Arguments();
                jobArgs.put(JobService.jobStartedAt, jobRec.getJobStartedAt());
                jobItem.job.addToJobArguments(jobArgs);
                val nextPStep = jobItem.stepsMap.get(jobRec.getCurrentStepName());

                if(nextPStep != null) {
                    sendStepExecutionInstruction(nextPStep, jobArgs, jobRec);
                }
                else { //No more steps, job complete
                    jobRec.setStatus(JobRecord.Status.Completed);
                    jobRec.setJobCompletedAt(now);
                }
            }
        }
    }

    //----------------------------------------------------------------------
    private void sendStepExecutionInstruction(PipelineStep pipelineStep,
                                              Arguments jobArgs,
                                              JobRecord jobRecord){
        //Get the partitions for the step
        val step = pipelineStep.step;
        val partArgs = step.getPartitionArguments(jobArgs);

        int totalPartitions = 1;
        if(partArgs!=null && !partArgs.isEmpty()){
            if(partArgs.size() < 2) throw new SysAgentException("Minimum partitions is 2");
            totalPartitions = partArgs.size();
        }

        log.debug("Total partitions = "+totalPartitions);
        for(int i=0; i < totalPartitions; i++){
            val stepRecord = new StepRecord();
            stepRecord.setJobRecordId(jobRecord.getId());
            stepRecord.setJobName(jobRecord.getJobName());
            stepRecord.setStepName(step.getName());
            stepRecord.setJobArguments(jobArgs);
            if(totalPartitions > 1) {
                val partArg = partArgs.get(i);
                stepRecord.setPartitionArguments(partArg);
                stepRecord.setPartitionNum(i);
                stepRecord.setPartitionCount(totalPartitions);
            }
            mongoTemplate.save(stepRecord);
        }

        jobRecord.setCurrentStepName(step.getName());
        jobRecord.setCurrentStepPartitionCount(totalPartitions);
        mongoTemplate.save(jobRecord);
    }

    //-----------------------------------------------
    public void initialise(NodeInfo nodeInfo){

        val beanFactory = appContext.getBeanFactory();
        val jobBeanNames = beanFactory.getBeanNamesForType(Job.class);
        for (val beanName : jobBeanNames) {
            val jobBean = beanFactory.getBean(beanName, Job.class);
            val jobItem = new JobItem();
            jobItem.job = jobBean;
            val pipeline = jobBean.getPipeline();
            val firstPStep = pipeline.getFirstStep();
            if(firstPStep==null){
                throw new SysAgentException("First step is missing in pipeline for job "+jobBean.getName());
            }
            jobItem.firstStep = firstPStep;

            //Create a map of step name to pipeline step
            var currPStep = firstPStep;
            do {
                jobItem.stepsMap.put(currPStep.step.getName(), currPStep);
                currPStep = currPStep.nextPipelineStep;
            }
            while(currPStep!=null);

            jobsMap.put(jobBean.getName(), jobItem);
        }

        //MongoDB indices
        mongoTemplate.indexOps(JobRecord.class)
                .ensureIndex(new Index()
                        .on("jobRecordId", Sort.Direction.ASC)
                        .on("stepName", Sort.Direction.ASC)
                        .on("status", Sort.Direction.ASC));


        log.debug("JobService initialised");
    }

    public static final String jobStartedAt = "jobStartedAt";
}
