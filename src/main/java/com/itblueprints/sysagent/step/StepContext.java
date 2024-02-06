package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import lombok.Getter;

@Getter
public class StepContext {

    private final Arguments arguments = new Arguments();

    private Integer partitionNum;
    void setPartitionNum(Integer partitionNum) {this.partitionNum = partitionNum;}

    private Integer partitionCount;
    void setPartitionCount(Integer partitionCount) {this.partitionCount = partitionCount;}

}
