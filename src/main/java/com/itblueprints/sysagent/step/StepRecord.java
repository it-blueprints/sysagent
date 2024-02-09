package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.ExecutionStatus;
import com.itblueprints.sysagent.job.JobArguments;
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
public class StepRecord {

    @Id
    private String id;

    @Indexed
    private String jobRecordId;

    private String jobName;

    @Indexed
    private String stepName;

    private Partition partition;

    private JobArguments jobArguments;

    private boolean claimed;

    private String nodeId;

    private String claimedAt;

    private LocalDateTime startedAt;

    private LocalDateTime completedAt;

    private LocalDateTime lastUpdateAt;

    private long batchItemsProcessed = -1;

    private int retryCount = 0;

    @Indexed
    private ExecutionStatus status = ExecutionStatus.NEW;

    public static StepRecord of(String jobRecordId, String jobName, String stepName, JobArguments jobArguments){
        val sr = new StepRecord();
        sr.jobRecordId = jobRecordId;
        sr.jobName = jobName;
        sr.stepName = stepName;
        sr.jobArguments = jobArguments;
        return sr;
    }

}
