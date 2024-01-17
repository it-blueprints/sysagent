package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.SystemAgentException;
import com.itblueprints.sysagent.step.Step;
import lombok.Getter;
import lombok.val;

import java.util.LinkedList;

@Getter
public class JobPipeline {

    //-------------------------------------------
    private Arguments arguments = new Arguments();

    private JobPipeline(){}

    public static JobPipeline create(){
        return new JobPipeline();
    }

    public JobPipeline withArgument(String key, Object value){
        arguments.put(key, value);
        return this;
    }

    //-------------------------------------------
    private LinkedList<PipelineStep> steps = new LinkedList<>();

    public JobPipeline withFirstStep(Step step){
        val pStep = new PipelineStep();
        pStep.step = step;
        if(!steps.isEmpty()){
            throw new SystemAgentException("Cannot add first step as the pipeline is not empty");
        }
        steps.add(pStep);
        return this;
    }

    //-------------------------------------------
    public JobPipeline withNextStep(Step step){
        val pStep = new PipelineStep();
        pStep.step = step;
        if(steps.isEmpty()){
            throw new SystemAgentException("Cannot add next step as the pipeline is empty. Use withFirstStep() first");
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
