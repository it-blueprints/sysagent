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