package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.ExecutionStatus;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
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
    private ExecutionStatus status;

    private JobArguments jobArguments;

    private LocalDateTime startedAt;

    private LocalDateTime completedAt;

    private LocalDateTime lastUpdateAt;

    private String currentStepName;

    private int currentStepPartitionCount = 0;

    private int currentStepPartitionsCompletedCount = 0;

    private int currentStepRetryCount = 0;

    //------------------------------------------
    public static JobRecord of(String jobName, JobArguments jobArguments, LocalDateTime startedAt) {
        val jr = new JobRecord();
        jr.jobName = jobName;
        jr.status = ExecutionStatus.RUNNING;
        jr.jobArguments = jobArguments;
        jr.startedAt = startedAt;
        jr.lastUpdateAt = startedAt;
        return jr;
    }
}
