/*
 * Copyright 2023-2024 the original author or authors.
 *
 * Licensed under the Apache Software License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
