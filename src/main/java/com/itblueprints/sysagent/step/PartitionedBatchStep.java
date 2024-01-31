package com.itblueprints.sysagent.step;

public interface PartitionedBatchStep<IN, OUT> extends StepI, Batched<IN, OUT>, Partitioned {
}
