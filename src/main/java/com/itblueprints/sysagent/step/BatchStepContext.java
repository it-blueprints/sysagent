package com.itblueprints.sysagent.step;

import lombok.Getter;

@Getter
public class BatchStepContext extends StepContext{

    private long itemsProcessed;
    void setItemsProcessed(long itemsProcessed) {
        this.itemsProcessed = itemsProcessed;
    }

}
