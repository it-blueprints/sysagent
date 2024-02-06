package com.itblueprints.sysagent.job;

public class MockJob implements Job {

    @Override
    public JobPipeline getPipeline() {
        return JobPipeline.create()
                .firstStep(new MockStep1())
                .nextStep(new MockStep2());

    }

    @Override
    public String getName() {
        return "Job";
    }
}
