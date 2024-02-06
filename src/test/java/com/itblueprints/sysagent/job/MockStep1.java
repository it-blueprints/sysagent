package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.step.PartitionedStep;
import com.itblueprints.sysagent.step.StepContext;

import java.util.List;

public class MockStep1 implements PartitionedStep {
    @Override
    public List<Arguments> getPartitionArgumentsList(Arguments jobArguments) {
        return List.of(
                Arguments.of("k1","k1v1", "k2", "k2v1"),
                Arguments.of("k1","k1v2", "k2", "k2v1"),
                Arguments.of("k1","k1v3", "k2", "k2v2"),
                Arguments.of("k1","k1v4", "k2", "k2v2")
        );
    }

    @Override
    public void run(StepContext context) {

    }

    @Override
    public String getName() {
        return "Step1";
    }
}
