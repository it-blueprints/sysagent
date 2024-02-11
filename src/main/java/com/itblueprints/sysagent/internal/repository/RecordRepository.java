package com.itblueprints.sysagent.internal.repository;

import com.itblueprints.sysagent.internal.cluster.BaseNodeRecord;
import com.itblueprints.sysagent.internal.cluster.ManagerNodeRecord;
import com.itblueprints.sysagent.internal.cluster.NodeRecord;
import com.itblueprints.sysagent.internal.job.JobRecord;
import com.itblueprints.sysagent.internal.step.StepRecord;

import java.util.List;

public interface RecordRepository {

    //Node Record
    <T extends BaseNodeRecord> T save(T nodeRecord);
    ManagerNodeRecord getManagerNodeRecord();
    NodeRecord getNodeRecordById(String id);
    ManagerNodeRecord tryGetLockedManagerNodeRecord();
    List<NodeRecord> getRecordsForOtherNodes(String thisNodeId);
    void delete(BaseNodeRecord nodeRecord);

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
