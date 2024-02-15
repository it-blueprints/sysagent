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

package com.itblueprints.sysagent.internal.step;

import com.itblueprints.sysagent.internal.ExecutionStatus;
import com.itblueprints.sysagent.job.JobArguments;
import com.itblueprints.sysagent.step.Partition;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Getter
@Setter
@Document
public class StepRecord {

    @Id
    private String id;

    @Indexed
    private String jobRecordId;

    private String jobName;

    @Indexed
    private String stepName;

    private Partition partition;

    private JobArguments jobArguments;

    private boolean claimed;

    private String nodeId;

    private String claimedAt;

    private LocalDateTime startedAt;

    private LocalDateTime completedAt;

    private LocalDateTime lastUpdateAt;

    private long batchItemsProcessed = -1;

    private int retryCount = 0;

    @Indexed
    private ExecutionStatus status = ExecutionStatus.NEW;

    public static StepRecord of(String jobRecordId, String jobName, String stepName, JobArguments jobArguments){
        val sr = new StepRecord();
        sr.jobRecordId = jobRecordId;
        sr.jobName = jobName;
        sr.stepName = stepName;
        sr.jobArguments = jobArguments;
        return sr;
    }

}
