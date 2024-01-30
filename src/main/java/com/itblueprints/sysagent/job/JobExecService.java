package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.*;
import com.itblueprints.sysagent.cluster.ClusterInfo;
import com.itblueprints.sysagent.repository.RecordRepository;
import com.itblueprints.sysagent.step.Step;
import com.itblueprints.sysagent.step.StepRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobExecService {

    private final ConfigurableApplicationContext appContext;
    private final RecordRepository repository;
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
    // Called either by the SchedulerService or via API
    public void runJob(String jobName, Arguments jobArgs) {

        if(!jobsMap.containsKey(jobName)){
            throw new SysAgentException("Job with name '"+jobName+"' not found");
        }

        log.debug("Running "+jobName);

        if(!jobArgs.contains(SysAgentService.DataKeys.jobStartedAt)){
            jobArgs.put(SysAgentService.DataKeys.jobStartedAt, LocalDateTime.now());
        }

        val jobStartedAt = jobArgs.asTime(SysAgentService.DataKeys.jobStartedAt);

        val jRec = new JobRecord();
        jRec.setJobName(jobName);
        jRec.setJobArguments(jobArgs);
        jRec.setStatus(ExecStatus.Executing);
        jRec.setStartedAt(jobStartedAt);
        jRec.setLastUpdateAt(jobStartedAt);

        var jobRec = repository.save(jRec);

        val jobItem = jobsMap.get(jobName);
        val firstStep = jobItem.firstStep;
        jobItem.job.onStart(jobArgs);

        sendStepExecutionInstruction(firstStep, jobArgs, jobRec);
        repository.save(jobRec);
    }

    //------------------------------------------------------------
    public void onHeartBeat(ClusterInfo clusterInfo, LocalDateTime now) {
        processExecutingJobs(now);
        releaseDeadClaims(clusterInfo);
    }

    //--------------------------------------------------------------------
    void processExecutingJobs(LocalDateTime now) {
        val executingJobs = repository.findExecutingJobRecords();
        for(val jobRec : executingJobs){

            threadManager.getExecutor().submit(() -> {
                try {
                    doIfCurrentStepIsComplete(jobRec, now);
                    doIfJobHasFailed(jobRec, now);
                }
                catch (Exception e){
                    e.printStackTrace();
                    throw new SysAgentException("Error processing executing job "+jobRec.getJobName(), e);
                }
            });
        }
    }

    //--------------------------------------------------------------------
    private void doIfCurrentStepIsComplete(JobRecord jobRec, LocalDateTime now) {

        val completedPrtns = repository.findCompletedPartitionsOfCurrentStepOfJob(jobRec.getId(), jobRec.getCurrentStepName());
        val partitionsCompletedCount = completedPrtns.size();
        jobRec.setPartitionsCompletedCount(partitionsCompletedCount);

        log.debug("partitions completed = "+partitionsCompletedCount+" of "+jobRec.getPartitionCount());

        //If all partitions of current step are complete,
        if(partitionsCompletedCount == jobRec.getPartitionCount()){

            //Get next step if present
            val jobItem = jobsMap.get(jobRec.getJobName());
            val nextPStep = jobItem.getStep(jobRec.getCurrentStepName()).nextPipelineStep;

            val jobArgs = jobRec.getJobArguments();
            if(nextPStep != null) {
                //Next step
                log.debug("Sending step execution instruction for step - "+nextPStep.stepName);
                sendStepExecutionInstruction(nextPStep, jobArgs, jobRec);
            }
            else { //No more steps, job complete
                jobItem.job.onComplete(jobArgs);
                log.debug("Job complete - "+jobRec.getJobName());
                jobRec.setStatus(ExecStatus.Completed);
                jobRec.setCompletedAt(now);
            }
        }

        jobRec.setLastUpdateAt(now);
        repository.save(jobRec);
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
            repository.save(stepRecord);
        }
    }

    //-------------------------------------------------------
    public void releaseDeadClaims(ClusterInfo clusterInfo){
        for(val deadNodeId : clusterInfo.deadNodeIds){
            val unworkedStepRecs = repository.findExecutingStepPartitionsOfNode(deadNodeId);
            for(val stepRec : unworkedStepRecs){
                stepRec.setClaimed(false);
                stepRec.setNodeId(null);
                repository.save(stepRec);
            }
        }
    }

    //---------------------------------------------------------------
    public void doIfJobHasFailed(JobRecord jobRec, LocalDateTime now) {

        val prtns = repository.findCompletedOrFailedStepsPartitionsOfJob(jobRec.getId());
        val prtnCount = prtns.size();

        //If all partitions of current step are either completed or failed, job has failed
        if(prtnCount == jobRec.getPartitionCount()){
            val hasFailuresOpt = prtns.stream().filter(p -> p.getStatus().equals(ExecStatus.Failed)).findAny();
            if(hasFailuresOpt.isPresent()) {
                jobRec.setStatus(ExecStatus.Failed);
                jobRec.setLastUpdateAt(now);
                repository.save(jobRec);
            }
        }
    }
    //-----------------------------------------------
    //Called from API
    public void retryFailedJob(String jobName, LocalDateTime now) {
        if(!jobsMap.containsKey(jobName)){
            throw new SysAgentException("Job not found. jobName="+jobName);
        }
        val failedJobRec = repository.findJobRecordForFailedJob(jobName);
        if(failedJobRec == null) throw new SysAgentException("Cannot retry Job as it is not marked as Failed. jobName="+jobName);
        log.debug("Retrying "+jobName);
        val failedStepPrtns = repository.findFailedStepPartitionsOfJob(failedJobRec.getId());
        for(val failedStepPrtn : failedStepPrtns){
            failedStepPrtn.setStatus(ExecStatus.New);
            failedStepPrtn.setClaimed(false);
            failedStepPrtn.setRetryCount(failedStepPrtn.getRetryCount()+1);
            failedStepPrtn.setLastUpdateAt(now);
            repository.save(failedStepPrtn);
        }
    }

    //-----------------------------------------------
    public void initialise(ClusterInfo clusterInfo){

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
                throw new SysAgentException("First step is missing in pipeline for Job "+jobBean.getName());
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
        repository.initialise();

        log.debug("JobService initialised");
    }

}
