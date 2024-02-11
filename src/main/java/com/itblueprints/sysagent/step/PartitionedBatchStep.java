package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.internal.step.Batched;
import com.itblueprints.sysagent.internal.step.Partitioned;

public interface PartitionedBatchStep<IN, OUT> extends Step, Batched<IN, OUT>, Partitioned {
}
