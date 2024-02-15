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

package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.internal.SysAgentException;
import com.itblueprints.sysagent.internal.job.PipelineStep;
import com.itblueprints.sysagent.step.Step;
import lombok.Getter;
import lombok.val;

public class JobPipeline {

   private JobPipeline(){}

    public static JobPipeline create(){
        return new JobPipeline();
    }

    //-------------------------------------------
    @Getter
    private PipelineStep firstStep ;

    private PipelineStep currentStep;

    public JobPipeline firstStep(Step step){
        if(firstStep!=null){
            throw new SysAgentException("Cannot add first step as the pipeline is not empty");
        }
        firstStep = new PipelineStep();
        firstStep.step = step;
        currentStep = firstStep;
        return this;
    }

    //-------------------------------------------
    public JobPipeline nextStep(Step step){
        if(currentStep==null){
            throw new SysAgentException("Cannot add next step as the pipeline is empty");
        }
        val pStep = new PipelineStep();
        pStep.step = step;
        currentStep.nextPipelineStep = pStep;
        currentStep = pStep;
        return this;
    }
}
