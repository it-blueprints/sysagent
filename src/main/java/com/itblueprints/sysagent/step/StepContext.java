package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.internal.MapData;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class StepContext extends MapData {

    private Integer partitionNum;

    private Integer totalPartitions;
}
