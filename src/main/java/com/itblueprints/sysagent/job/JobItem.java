package com.itblueprints.sysagent.job;

import java.util.HashMap;
import java.util.Map;

public class JobItem {
    public Job job;
    public PipelineStep firstStep;
    public Map<String, PipelineStep> stepsMap = new HashMap<>();
}
