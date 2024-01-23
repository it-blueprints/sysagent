package com.itblueprints.sysagent.job;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Getter
@Setter
@Document
public class JobRecord {

    @Id
    private String id;

    @Indexed
    private String jobName;

    private LocalDateTime jobStartedAt;

    @Indexed
    private Status status = Status.New;

    private LocalDateTime jobCompletedAt;

    private String currentStepName;

    private int currentStepPartitionCount = 0;

    //------------------
    public enum Status {
        New,
        Executing,
        Completed
    }
}
