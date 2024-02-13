package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.internal.step.Batched;

public interface BatchStep<IN, OUT> extends Step, Batched<IN, OUT> {
}
