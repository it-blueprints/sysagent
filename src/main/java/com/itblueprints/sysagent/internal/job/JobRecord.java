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

package com.itblueprints.sysagent.internal.job;

import com.itblueprints.sysagent.internal.ExecutionStatus;
import com.itblueprints.sysagent.job.JobArguments;
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
public class JobRecord {

    @Id
    private String id;

    private String jobName;

    @Indexed
    private ExecutionStatus status;

    private JobArguments jobArguments;

    private LocalDateTime startedAt;

    private LocalDateTime completedAt;

    private LocalDateTime lastUpdateAt;

    private String currentStepName;

    private int currentStepPartitionCount = 0;

    private int currentStepPartitionsCompletedCount = 0;

    private int currentStepRetryCount = 0;

    //------------------------------------------
    public static JobRecord of(String jobName, JobArguments jobArguments, LocalDateTime startedAt) {
        val jr = new JobRecord();
        jr.jobName = jobName;
        jr.status = ExecutionStatus.RUNNING;
        jr.jobArguments = jobArguments;
        jr.startedAt = startedAt;
        jr.lastUpdateAt = startedAt;
        return jr;
    }
}
