package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.step.Step;

import java.util.HashMap;
import java.util.Map;

public class JobItem {
    public Job job;
    public JobPipeline pipeline;
    public Map<String, Step> stepsMap = new HashMap<>();
}
