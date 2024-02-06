package com.itblueprints.sysagent.job;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CurrentStepDetails {

    private String stepName;

    private int partitionCount = 0;

    private int partitionsCompletedCount = 0;

    private int retryCount = 0;
}
