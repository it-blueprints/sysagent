package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.step.StepI;

class PipelineStep {
    public StepI step;
    public String onOutcome;
    public PipelineStep nextPipelineStep;
}
