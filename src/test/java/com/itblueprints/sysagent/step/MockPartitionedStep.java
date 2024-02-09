package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.job.JobArguments;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class MockPartitionedStep implements PartitionedStep {

    boolean runCalled = false;
    StepContext stepContext;


    @Override
    public List<Partition> getPartitions(JobArguments jobArguments) {
        return Stream.of(1,2,3).map(i -> Partition.from(Map.of("partition", i))).toList();
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
