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

    @Indexed
    private LocalDateTime jobStartedAt;

    private int totalPartitions = 0;

    private Status status = Status.New;

    public enum Status {
        New,
        Executing,
        Failed,
        Completed
    }
}
