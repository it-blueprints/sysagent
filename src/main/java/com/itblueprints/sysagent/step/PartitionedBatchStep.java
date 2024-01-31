package com.itblueprints.sysagent.step;

public interface PartitionedBatchStep<IN, OUT> extends Step, Batched<IN, OUT>, Partitioned {
}
