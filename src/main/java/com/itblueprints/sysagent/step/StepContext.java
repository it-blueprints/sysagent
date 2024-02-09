package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.job.JobArguments;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter(AccessLevel.PACKAGE)
public class StepContext {

    private JobArguments jobArguments;

    private Partition partition;

}
