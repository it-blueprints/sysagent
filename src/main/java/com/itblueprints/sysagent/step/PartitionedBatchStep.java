package com.itblueprints.sysagent.step;

public interface PartitionedBatchStep<IN, OUT> extends BatchStep<IN, OUT>, Partitioned {
}
