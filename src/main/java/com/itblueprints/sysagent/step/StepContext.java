package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import lombok.Getter;

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

    private StepDataMap stepDataMap = new StepDataMap();

}
