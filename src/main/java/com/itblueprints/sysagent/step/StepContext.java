package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import lombok.Getter;
import lombok.Setter;

@Getter
public class StepContext {

    private Arguments arguments = new Arguments();

    private Integer partitionNum;
    void setPartitionNum(Integer partitionNum) {this.partitionNum = partitionNum;}

    private Integer totalPartitions;
    void setTotalPartitions(Integer totalPartitions) {this.totalPartitions = totalPartitions;}

    private long itemsProcessed;
    void setItemsProcessed(long itemsProcessed) {
        this.itemsProcessed = itemsProcessed;
    }

    @Setter
    private CheckPointState checkPointState;

    private AuxiliaryData auxiliaryData = new AuxiliaryData();

}
