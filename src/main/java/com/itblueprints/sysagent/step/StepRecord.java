package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Getter
@Setter
@Document
public class StepRecord {

    @Id
    private String id;

    @Indexed
    private String jobRecordId;

    private String jobName;

    private String stepName;

    private Arguments partitionArguments;

    private Integer partitionNum;

    private int totalPartitions = 0;

    private Arguments jobArguments;

    private boolean claimed;

    private String nodeId;

    private String claimedAt;

    private LocalDateTime startedAt;

    private LocalDateTime completedAt;

    private Status status = Status.New;

    private CheckPointState checkPointState;

    public enum Status {
        New,
        Executing,
        Failed,
        Completed
    }
}
