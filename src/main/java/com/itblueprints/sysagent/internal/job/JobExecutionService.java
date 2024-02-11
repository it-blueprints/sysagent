package com.itblueprints.sysagent.internal.job;

import com.itblueprints.sysagent.*;
import com.itblueprints.sysagent.internal.cluster.NodeInfo;
import com.itblueprints.sysagent.internal.ExecutionStatus;
import com.itblueprints.sysagent.internal.SysAgentException;
import com.itblueprints.sysagent.internal.ThreadManager;
import com.itblueprints.sysagent.job.Job;
import com.itblueprints.sysagent.job.JobArguments;
import com.itblueprints.sysagent.internal.repository.RecordRepository;
import com.itblueprints.sysagent.internal.step.Partitioned;
import com.itblueprints.sysagent.step.Step;
import com.itblueprints.sysagent.internal.step.StepRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobExecutionService {

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
    public void runJob(String jobName, JobArguments jobArgs) {

        if(!jobsMap.containsKey(jobName)){
            throw new SysAgentException("Job with name '"+jobName+"' not found");
        }

        log.debug("Running "+jobName);

        if(!jobArgs.contains(SysAgentService.DataKeys.jobStartedAt)){
            jobArgs.put(SysAgentService.DataKeys.jobStartedAt, LocalDateTime.now());
        }

        val jobStartedAt = jobArgs.getTime(SysAgentService.DataKeys.jobStartedAt);

        val jRec = JobRecord.of(jobName, jobArgs, jobStartedAt);
        var jobRec = repository.save(jRec);

        val jobItem = jobsMap.get(jobName);
        jobItem.job.onStart(jobArgs);

        sendStepExecutionInstruction(jobItem.firstStep.step, jobArgs, jobRec);
        repository.save(jobRec);
    }

    //------------------------------------------------------------
    public void onHeartBeat(NodeInfo nodeInfo, LocalDateTime now) {
        processExecutingJobs(now);
        releaseDeadClaims(nodeInfo);
    }

    //--------------------------------------------------------------------
    void processExecutingJobs(LocalDateTime now) {
        val executingJobs = repository.getRunningJobRecords();
        for(val jobRec : executingJobs){

            threadManager.getExecutor().submit(() -> {
                try {
                    val stepRecs = repository.getStepsRecordsForStepOfJob(jobRec.getId(), jobRec.getCurrentStepName());
                    if(isCurrentStepComplete(jobRec, stepRecs)){
                        //get next step
                        val jobItem = jobsMap.get(jobRec.getJobName());
                        val nextPStep = jobItem.getStep(jobRec.getCurrentStepName()).nextPipelineStep;
                        val jobArgs = jobRec.getJobArguments();
                        if(nextPStep != null) { //there is a next step
                            sendStepExecutionInstruction(nextPStep.step, jobArgs, jobRec);
                        }
                        else { //No more steps, job complete
                            jobItem.job.onComplete(jobArgs);
                            log.debug("Job complete - "+jobRec.getJobName());
                            jobRec.setStatus(ExecutionStatus.COMPLETE);
                            jobRec.setCompletedAt(now);
                        }
                        jobRec.setLastUpdateAt(now);
                        repository.save(jobRec);
                    }
                    else if(hasCurrentStepFailed(jobRec, stepRecs)){
                        //Mark job as failed
                        jobRec.setStatus(ExecutionStatus.FAILED);
                        jobRec.setLastUpdateAt(now);
                        repository.save(jobRec);
                    }

                }
                catch (Exception e){
                    e.printStackTrace();
                    throw new SysAgentException("Error processing executing job "+jobRec.getJobName(), e);
                }
            });
        }
    }

    //-----------------------------------------------------------------------------
    boolean isCurrentStepComplete(JobRecord jobRec, List<StepRecord> stepRecs){
        if(stepRecs==null || stepRecs.isEmpty()){
            throw new SysAgentException("There must be one or more step records at this stage");
        }
        boolean completed = false;
        val currentStepName = jobRec.getCurrentStepName();
        if(jobRec.getCurrentStepPartitionCount() > 0){
            val completedCount = stepRecs.stream()
                    .filter(sr -> sr.getStepName().equals(currentStepName)
                            && sr.getStatus() == ExecutionStatus.COMPLETE)
                    .count();
            jobRec.setCurrentStepPartitionsCompletedCount(Math.toIntExact(completedCount));
            log.debug("partitions completed = "+completedCount+" of "+jobRec.getCurrentStepPartitionCount());
            completed = completedCount == jobRec.getCurrentStepPartitionCount();
        }
        else {
            completed = (stepRecs.size() == 1) && (stepRecs.get(0).getStatus() == ExecutionStatus.COMPLETE);
        }
        return completed;
    }

    //----------------------------------------------
    boolean hasCurrentStepFailed(JobRecord jobRec, List<StepRecord> stepRecs){

        if(stepRecs==null || stepRecs.isEmpty()){
            throw new SysAgentException("There must be one or more step records at this stage");
        }

        boolean failed = false;
        if(jobRec.getCurrentStepPartitionCount() > 0){
            val completeCount = stepRecs.stream()
                    .filter(sr -> sr.getStatus() == ExecutionStatus.COMPLETE)
                    .count();

            val failedCount = stepRecs.stream()
                    .filter(sr -> sr.getStatus() == ExecutionStatus.FAILED )
                    .count();

            failed = (completeCount+failedCount == jobRec.getCurrentStepPartitionCount()) && failedCount > 0;
        }
        else {
            failed = stepRecs.get(0).getStatus() == ExecutionStatus.FAILED;
        }
        return failed;
    }

    //----------------------------------------------------------------------
    void sendStepExecutionInstruction(Step step,
                                      JobArguments jobArgs,
                                      JobRecord jobRecord){

        log.debug("Sending step execution instruction for step - " + step.getName());
        jobRecord.setCurrentStepName(step.getName());

        if(step instanceof Partitioned) {

            val prtned = (Partitioned) step;
            val partitions = prtned.getPartitions(jobArgs);

            int prtnCount = 0;
            if (partitions != null && !partitions.isEmpty()) {
                if (partitions.size() < 2) throw new SysAgentException("Minimum partitions is 2");
                prtnCount = partitions.size();
            }
            jobRecord.setCurrentStepPartitionCount(prtnCount);
            jobRecord.setCurrentStepPartitionsCompletedCount(0);
            log.debug("Total partitions = " + prtnCount);

            for(int i=0; i < prtnCount; i++){
                val prtn = partitions.get(i);
                prtn.setPartitionNum(i);
                prtn.setTotalPartitions(prtnCount);
                val stepRecord = StepRecord.of(jobRecord.getId(), jobRecord.getJobName(), step.getName(), jobArgs);
                stepRecord.setPartition(prtn);
                repository.save(stepRecord);
            }

        }
        else {
            val stepRecord = StepRecord.of(jobRecord.getId(), jobRecord.getJobName(), step.getName(), jobArgs);
            repository.save(stepRecord);
        }

    }

    //-------------------------------------------------------
    public void releaseDeadClaims(NodeInfo nodeInfo){
        for(val deadNodeId : nodeInfo.deadNodeIds){
            val unworkedStepRecs = repository.getStepRecordsClaimedByNode(deadNodeId);
            for(val stepRec : unworkedStepRecs){
                stepRec.setClaimed(false);
                stepRec.setNodeId(null);
                repository.save(stepRec);
            }
        }
    }

    //-----------------------------------------------
    //Called from API
    public void retryFailedJob(String jobName, LocalDateTime now) {
        if(!jobsMap.containsKey(jobName)){
            throw new SysAgentException("Job not found. jobName="+jobName);
        }
        val failedJobRec = repository.getFailedJobRecordOfJob(jobName);
        if(failedJobRec == null) throw new SysAgentException("Cannot retry Job as it is not marked as Failed. jobName="+jobName);
        log.debug("Retrying "+jobName);
        val failedStepRecs = repository.getFailedStepRecordsForJob(failedJobRec.getId());
        for(val failedSr : failedStepRecs){
            failedSr.setStatus(ExecutionStatus.NEW);
            failedSr.setClaimed(false);
            failedSr.setRetryCount(failedSr.getRetryCount()+1);
            failedSr.setLastUpdateAt(now);
            repository.save(failedSr);
        }
    }

    //-----------------------------------------------
    public void initialise(NodeInfo nodeInfo){

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
                jobItem.putStep(currPStep.step.getName(), currPStep);
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
