package com.itblueprints.sysagent.repository;

import com.itblueprints.sysagent.cluster.NodeRecord;
import com.itblueprints.sysagent.job.JobRecord;
import com.itblueprints.sysagent.step.StepRecord;

import java.util.List;

public interface RecordRepository {

    //Node Record
    NodeRecord save(NodeRecord nodeRecord);
    NodeRecord getManagerNodeRecord();
    NodeRecord getNodeRecordById(String id);
    NodeRecord tryGetLockedManagerNodeRecord();
    List<NodeRecord> getOtherNodeRecords(String thisNodeId);
    void delete(NodeRecord nodeRecord);

    //Job Record
    JobRecord save(JobRecord jobRecord);
    List<JobRecord> findExecutingJobRecords();
    void ensureJobRecordIndices();
    JobRecord findJobRecordForFailedJob(String jobName);

    //Step Record
    StepRecord save(StepRecord stepRecord);
    List<StepRecord> findCompletedPartitionsOfCurrentStepOfJob(String jobRecordId, String stepName);
    List<StepRecord> findExecutingStepPartitionsOfNode(String nodeRecordId);
    List<StepRecord> findCompletedOrFailedStepsPartitionsOfJob(String jobRecordId);
    List<StepRecord> findFailedStepPartitionsOfJob(String jobRecordId);
    StepRecord tryClaimNextStepPartition(String thisNodeId);
}
