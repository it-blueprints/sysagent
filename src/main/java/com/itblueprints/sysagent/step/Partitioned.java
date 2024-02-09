package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.job.JobArguments;

import java.util.List;

public interface Partitioned {

    List<Partition> getPartitions(JobArguments jobArguments);

}
