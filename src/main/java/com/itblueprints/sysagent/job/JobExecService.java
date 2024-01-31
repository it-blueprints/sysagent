package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.*;
import com.itblueprints.sysagent.cluster.ClusterInfo;
import com.itblueprints.sysagent.repository.RecordRepository;
import com.itblueprints.sysagent.step.Partitioned;
import com.itblueprints.sysagent.step.StepI;
import com.itblueprints.sysagent.step.StepRecord;
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
public class JobExecService {

    private final ConfigurableApplicationContext appContext;
    private final RecordRepository repository;
    private final ThreadManager threadManager;

    //--------------------------------------------------------------
    private Map<String, JobItem> jobsMap = new HashMap<>();

    public StepI getStep(String jobName, String stepName){
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

        val jRec = JobRecord.create(jobName, jobArgs, jobStartedAt);
        var jobRec = repository.save(jRec);

        val jobItem = jobsMap.get(jobName);
        jobItem.job.onStart(jobArgs);

        sendStepExecutionInstruction(jobItem.firstStep.step, jobArgs, jobRec);
        repository.save(jobRec);
    }

    //------------------------------------------------------------
    public void onHeartBeat(ClusterInfo clusterInfo, LocalDateTime now) {
        processExecutingJobs(now);
        releaseDeadClaims(clusterInfo);
    }

    //--------------------------------------------------------------------
    void processExecutingJobs(LocalDateTime now) {
        val executingJobs = repository.findRecordsForRunningJobs();
        for(val jobRec : executingJobs){

            threadManager.getExecutor().submit(() -> {
                try {
                    val stepRecs = repository.getRecordsOfStepOfJob(jobRec.getId(), jobRec.getCurrentStepName());
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
                            jobRec.setStatus(ExecStatus.COMPLETE);
                            jobRec.setCompletedAt(now);
                        }
                        jobRec.setLastUpdateAt(now);
                        repository.save(jobRec);
                    }
                    else if(hasCurrentStepFailed(jobRec, stepRecs)){
                        //Mark job as failed
                        jobRec.setStatus(ExecStatus.FAILED);
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
        boolean completed = false;
        if(jobRec.getPartitionCount() > 0){
            val completedCount = stepRecs.stream().filter(sr -> sr.getStatus() == ExecStatus.COMPLETE).count();
            jobRec.setPartitionsCompletedCount(Math.toIntExact(completedCount));
            log.debug("partitions completed = "+completedCount+" of "+jobRec.getPartitionCount());
            completed = completedCount == jobRec.getPartitionCount();
        }
        else {
            completed = stepRecs.get(0).getStatus() == ExecStatus.COMPLETE;
        }
        return completed;
    }

    //----------------------------------------------
    boolean hasCurrentStepFailed(JobRecord jobRec, List<StepRecord> stepRecs){

        boolean failed = false;
        if(jobRec.getPartitionCount() > 0){
            val completeCount = stepRecs.stream()
                    .filter(sr -> sr.getStatus() == ExecStatus.COMPLETE)
                    .count();

            val failedCount = stepRecs.stream()
                    .filter(sr -> sr.getStatus() == ExecStatus.FAILED )
                    .count();

            failed = (completeCount+failedCount == jobRec.getPartitionCount()) && failedCount > 0;
        }
        else {
            failed = stepRecs.get(0).getStatus() == ExecStatus.FAILED;
        }
        return failed;
    }

    //----------------------------------------------------------------------
    private void sendStepExecutionInstruction(StepI step,
                                              Arguments jobArgs,
                                              JobRecord jobRecord){

        log.debug("Sending step execution instruction for step - " + step.getName());
        jobRecord.setCurrentStepName(step.getName());

        if(step instanceof Partitioned) {

            val prtned = (Partitioned) step;
            val partArgs = prtned.getPartitionArgumentsList(jobArgs);

            int prtnCount = 0;
            if (partArgs != null && !partArgs.isEmpty()) {
                if (partArgs.size() < 2) throw new SysAgentException("Minimum partitions is 2");
                prtnCount = partArgs.size();
            }
            jobRecord.setPartitionCount(prtnCount);
            jobRecord.setPartitionsCompletedCount(0);
            log.debug("Total partitions = " + prtnCount);

            for(int i=0; i < prtnCount; i++){
                val stepRecord = StepRecord.create(jobRecord.getId(), jobRecord.getJobName(), step.getName(), jobArgs);
                val partArg = partArgs.get(i);
                stepRecord.setPartitionArguments(partArg);
                stepRecord.setPartitionNum(i);
                stepRecord.setPartitionCount(prtnCount);
                repository.save(stepRecord);
            }

        }
        else {
            val stepRecord = StepRecord.create(jobRecord.getId(), jobRecord.getJobName(), step.getName(), jobArgs);
            repository.save(stepRecord);
        }

    }

    //-------------------------------------------------------
    public void releaseDeadClaims(ClusterInfo clusterInfo){
        for(val deadNodeId : clusterInfo.deadNodeIds){
            val unworkedStepRecs = repository.findRunningStepPartitionsOfNode(deadNodeId);
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
        val failedJobRec = repository.findRecordForFailedJob(jobName);
        if(failedJobRec == null) throw new SysAgentException("Cannot retry Job as it is not marked as Failed. jobName="+jobName);
        log.debug("Retrying "+jobName);
        val failedStepPrtns = repository.findFailedStepPartitionsOfJob(failedJobRec.getId());
        for(val failedStepPrtn : failedStepPrtns){
            failedStepPrtn.setStatus(ExecStatus.NEW);
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
