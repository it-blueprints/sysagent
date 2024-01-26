package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.ClusterState;
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
public class JobExecService {

    private final ConfigurableApplicationContext appContext;
    private final MongoTemplate mongoTemplate;
    private final ThreadManager threadManager;

    //--------------------------------------------------------------
    private Map<String, JobItem> jobsMap = new HashMap<>();

    public Step getStep(String jobName, String stepName){
        if(jobsMap.containsKey(jobName)) {
            val jobItem = jobsMap.get(jobName);
            return jobItem.getStep(stepName).step;
        }
        else throw new SysAgentException("Job "+jobName+" not found");
    }


    //--------------------------------------------------------
    // Starts off a Job
    // Called either by the SchedulerService on by a API
    public void runJob(String jobName, Arguments jobArgs) {

        if(!jobsMap.containsKey(jobName)){
            throw new SysAgentException("Job with name '"+jobName+"' not found");
        }

        log.debug("Running "+jobName);

        if(!jobArgs.contains(Keys.jobStartedAt)){
            jobArgs.put(Keys.jobStartedAt, LocalDateTime.now());
        }

        val jobStartedAt = jobArgs.asTime(Keys.jobStartedAt);

        val jRec = new JobRecord();
        jRec.setJobName(jobName);
        jRec.setJobArguments(jobArgs);
        jRec.setStatus(JobRecord.Status.Executing);

        var jobRec = mongoTemplate.save(jRec);

        val jobItem = jobsMap.get(jobName);
        val firstStep = jobItem.firstStep;
        jobItem.job.addToJobArguments(jobArgs);

        sendStepExecutionInstruction(firstStep, jobArgs, jobRec);
        mongoTemplate.save(jobRec);
    }

    //------------------------------------------------------------
    public void onHeartBeat(ClusterState clusterState, LocalDateTime now) {
        processExecutingJobs(now);
        releaseDeadClaims(clusterState);
    }

    //--------------------------------------------------------------------
    void processExecutingJobs(LocalDateTime now) {
        val query = new Query();
        query.addCriteria(Criteria
                .where("status").is(JobRecord.Status.Executing));
        val executingJobs = mongoTemplate.find(query, JobRecord.class);
        for(val jobRec : executingJobs){

            threadManager.getExecutor().submit(() -> {
                try {
                    processExecutingJob(jobRec, now);
                }
                catch (Exception e){
                    e.printStackTrace();
                    throw new SysAgentException("Error processing executing job "+jobRec.getJobName(), e);
                }
            });
        }
    }

    //--------------------------------------------------------------------
    private void processExecutingJob(JobRecord jobRec, LocalDateTime now) {
        val query = new Query();
        query.addCriteria(Criteria
                .where("jobRecordId").is(jobRec.getId())
                .and("stepName").is(jobRec.getCurrentStepName())
                .and("status").is(StepRecord.Status.Completed)
        );
        val completedPrtns = mongoTemplate.find(query, StepRecord.class);
        val partitionsCompletedCount = completedPrtns.size();
        jobRec.setPartitionsCompletedCount(partitionsCompletedCount);

        log.debug("partitions completed = "+partitionsCompletedCount+" of "+jobRec.getPartitionCount());

        //If all partitions of current step are complete,
        if(partitionsCompletedCount == jobRec.getPartitionCount()){

            //Get next step if present
            val jobItem = jobsMap.get(jobRec.getJobName());
            val nextPStep = jobItem.getStep(jobRec.getCurrentStepName()).nextPipelineStep;

            if(nextPStep != null) {
                //Next step
                log.debug("Sending step execution instruction for step "+nextPStep.stepName);
                val jobArgs = jobRec.getJobArguments();
                sendStepExecutionInstruction(nextPStep, jobArgs, jobRec);
            }
            else { //No more steps, job complete
                log.debug("Job "+jobRec.getJobName()+" is complete");
                jobRec.setStatus(JobRecord.Status.Completed);
                jobRec.setJobCompletedAt(now);
            }
        }

        mongoTemplate.save(jobRec);
    }

    //----------------------------------------------------------------------
    private void sendStepExecutionInstruction(PipelineStep pipelineStep,
                                              Arguments jobArgs,
                                              JobRecord jobRecord){
        //Get the partitions for the step
        val step = pipelineStep.step;
        val partArgs = step.getPartitionArgumentsList(jobArgs);

        int totalPartitions = 1;
        if(partArgs!=null && !partArgs.isEmpty()){
            if(partArgs.size() < 2) throw new SysAgentException("Minimum partitions is 2");
            totalPartitions = partArgs.size();
        }

        log.debug("Total partitions = "+totalPartitions);

        jobRecord.setCurrentStepName(pipelineStep.stepName);
        jobRecord.setPartitionCount(totalPartitions);
        jobRecord.setPartitionsCompletedCount(0);

        for(int i=0; i < totalPartitions; i++){
            val stepRecord = new StepRecord();
            stepRecord.setJobRecordId(jobRecord.getId());
            stepRecord.setJobName(jobRecord.getJobName());
            stepRecord.setStepName(pipelineStep.stepName);
            stepRecord.setJobArguments(jobArgs);
            if(totalPartitions > 1) {
                val partArg = partArgs.get(i);
                stepRecord.setPartitionArguments(partArg);
                stepRecord.setPartitionNum(i);
                stepRecord.setPartitionCount(totalPartitions);
            }
            mongoTemplate.save(stepRecord);
        }
    }

    //-------------------------------------------------------
    public void releaseDeadClaims(ClusterState clusterState){
        for(val deadNodeId : clusterState.deadNodeIds){
            val query = new Query();
            query.addCriteria(Criteria
                    .where("nodeId").is(deadNodeId)
                    .and("status").is(StepRecord.Status.Executing)
            );
            val unworkedStepRecs = mongoTemplate.find(query, StepRecord.class);
            for(val stepRec : unworkedStepRecs){
                stepRec.setClaimed(false);
                stepRec.setNodeId(null);
                mongoTemplate.save(stepRec);
            }
        }
    }

    //-----------------------------------------------
    public void initialise(ClusterState clusterState){

        log.debug("Initialising JobService");
        val beanFactory = appContext.getBeanFactory();
        val jobBeanNames = beanFactory.getBeanNamesForType(Job.class);
        for (val beanName : jobBeanNames) {
            val jobBean = beanFactory.getBean(beanName, Job.class);
            val jobItem = new JobItem();
            jobItem.job = jobBean;
            log.debug("Loading pipeline for Job "+jobItem.job.getName());
            val pipeline = jobBean.getPipeline();
            val firstPStep = pipeline.getFirstStep();
            if(firstPStep==null){
                throw new SysAgentException("First step is missing in pipeline for job "+jobBean.getName());
            }
            jobItem.firstStep = firstPStep;

            //Create a map of step name to pipeline step
            var currPStep = firstPStep;
            do {
                jobItem.putStep(currPStep.stepName, currPStep);
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

    //--------------------------
    public static class Keys {
        public static final String jobStartedAt = "jobStartedAt";
    }
}
