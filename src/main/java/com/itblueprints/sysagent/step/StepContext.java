package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StepContext {

    private Arguments arguments = new Arguments();

    private StepData stepData;

    private CheckPointState checkPointState;

    private Integer partitionNum;

    private Integer totalPartitions;

    private long itemsProcessed;
    void setItemsProcessed(long itemsProcessed) {
        this.itemsProcessed = itemsProcessed;
    }



}
