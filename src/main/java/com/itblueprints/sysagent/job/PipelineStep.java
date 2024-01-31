package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.step.Step;

class PipelineStep {
    public Step step;
    public String onOutcome;
    public PipelineStep nextPipelineStep;
}
