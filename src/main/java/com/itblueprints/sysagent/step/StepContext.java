package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
public class StepContext {

    private Arguments jobArguments;

    private CheckPointState checkPointState;

    private Integer partitionNum;

    private Integer totalPartitions;

    private Arguments partitionArguments;
}
