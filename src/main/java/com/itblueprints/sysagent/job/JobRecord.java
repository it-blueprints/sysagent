package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.ExecStatus;
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

    private String jobName;

    @Indexed
    private ExecStatus status = ExecStatus.New;

    private Arguments jobArguments;

    private LocalDateTime startedAt;

    private LocalDateTime completedAt;

    private LocalDateTime lastUpdateAt;

    private String currentStepName;

    private int partitionCount = 0;

    private int partitionsCompletedCount = 0;

    private int retryCount = 0;

}
