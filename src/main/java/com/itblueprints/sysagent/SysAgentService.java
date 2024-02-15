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

package com.itblueprints.sysagent;

import com.itblueprints.sysagent.job.JobArguments;
import com.itblueprints.sysagent.internal.job.JobExecutionService;
import com.itblueprints.sysagent.internal.repository.RecordRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class SysAgentService {

    private final RecordRepository repository;
    private final JobExecutionService jobExecutionService;

    //--------------------------------
    public void resetCluster() {
        repository.clearAll();
    }

    //--------------------------------
    public void runJob(String jobName){
        runJob(jobName, new JobArguments());
    }

    //--------------------------------
    public void runJob(String jobName, JobArguments jobArguments){
        jobExecutionService.runJob(jobName, jobArguments);
    }


    //------------------------------------------------
    public void retryFailedJob(String jobName){
        jobExecutionService.retryFailedJob(jobName, LocalDateTime.now());
    }

    //--------------------------------
    public static class DataKeys {
        public static final String jobStartedAt = "jobStartedAt";
    }
}
