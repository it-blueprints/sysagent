package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.MapData;
import com.itblueprints.sysagent.job.JobArguments;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter(AccessLevel.PACKAGE)
@ToString(callSuper = true)
public class StepContext extends MapData {

    private Integer partitionNum;

    private Integer totalPartitions;
}
