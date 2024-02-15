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

import com.itblueprints.sysagent.internal.SysAgentException;
import com.itblueprints.sysagent.job.Job;

import java.util.HashMap;
import java.util.Map;

public class JobItem {
    public Job job;
    public PipelineStep firstStep;
    private Map<String, PipelineStep> stepsMap = new HashMap<>();

    public void putStep(String stepName, PipelineStep pStep){
        stepsMap.put(stepName, pStep);
    }

    public PipelineStep getStep(String stepName){
        if(!stepsMap.containsKey(stepName)) throw new SysAgentException("Step "+stepName+" not found");
        return stepsMap.get(stepName);
    }
}
