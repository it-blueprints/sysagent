package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.step.Step;
import lombok.Getter;
import lombok.val;

import java.util.LinkedList;

@Getter
public class JobPipeline {

   private JobPipeline(){}

    public static JobPipeline create(){
        return new JobPipeline();
    }

    //-------------------------------------------
    private LinkedList<PipelineStep> steps = new LinkedList<>();

    public JobPipeline withFirstStep(Step step){
        val pStep = new PipelineStep();
        pStep.step = step;
        if(!steps.isEmpty()){
            throw new SysAgentException("Cannot add first step as the pipeline is not empty");
        }
        steps.add(pStep);
        return this;
    }

    //-------------------------------------------
    public JobPipeline withNextStep(Step step){
        val pStep = new PipelineStep();
        pStep.step = step;
        if(steps.isEmpty()){
            throw new SysAgentException("Cannot add next step as the pipeline is empty. Use withFirstStep() first");
        }
        steps.add(pStep);
        return this;
    }

    //============================
    public static class PipelineStep {
        public Step step;
        public String onOutcome;
    }
}
