package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.step.Step;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PipelineStep {
    private Step step;
    private String onOutcome;
    private PipelineStep nextPipelineStep;
}
