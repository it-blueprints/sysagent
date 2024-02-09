package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockPartitionedStep implements PartitionedStep {

    boolean runCalled = false;
    StepContext stepContext;


    @Override
    public List<Arguments> getPartitionArgumentsList(Arguments jobArguments) {
        return List.of(1,2,3).stream().map(i -> Arguments.from(Map.of("partition", i))).collect(Collectors.toList());
    }

    @Override
    public void run(StepContext context) {
        runCalled = true;
        this.stepContext = context;
    }

    @Override
    public String getName() {
        return "Step";
    }
}
