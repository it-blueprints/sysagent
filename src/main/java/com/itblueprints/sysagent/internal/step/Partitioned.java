package com.itblueprints.sysagent.internal.step;

import com.itblueprints.sysagent.job.JobArguments;
import com.itblueprints.sysagent.step.Partition;

import java.util.List;

public interface Partitioned {

    List<Partition> getPartitions(JobArguments jobArguments);

}
