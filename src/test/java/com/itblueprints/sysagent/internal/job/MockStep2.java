package com.itblueprints.sysagent.internal.job;

import com.itblueprints.sysagent.step.SimpleStep;
import com.itblueprints.sysagent.step.StepContext;

public class MockStep2 implements SimpleStep {
    @Override
    public void run(StepContext context) {

    }

    @Override
    public String getName() {
        return "Step2";
    }
}
