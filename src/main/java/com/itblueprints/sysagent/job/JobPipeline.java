package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.SysAgentException;
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
        firstStep.stepName = step.getName();
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
        pStep.stepName = step.getName();
        currentStep.nextPipelineStep = pStep;
        currentStep = pStep;
        return this;
    }
}
