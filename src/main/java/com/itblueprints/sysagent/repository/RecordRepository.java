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
    List<NodeRecord> getRecordsForOtherNodes(String thisNodeId);
    void delete(NodeRecord nodeRecord);

    //Job Record
    JobRecord save(JobRecord jobRecord);
    List<JobRecord> getRunningJobRecords();
    void initialise();
    JobRecord getFailedJobRecordOfJob(String jobName);

    //Step Record
    StepRecord save(StepRecord stepRecord);
    List<StepRecord> getStepsRecordsForStepOfJob(String jobRecordId, String stepName);
    List<StepRecord> getStepRecordsClaimedByNode(String nodeRecordId);
    List<StepRecord> getFailedStepRecordsForJob(String jobRecordId);
    StepRecord tryClaimNextStepRecord(String thisNodeId);

    //Clear down db
    void clearAll();
}
