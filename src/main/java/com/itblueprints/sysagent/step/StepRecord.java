package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.ExecStatus;
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

    private Arguments partitionArguments;

    private Integer partitionNum;

    private int partitionCount = 0;

    private Arguments jobArguments;

    private boolean claimed;

    private String nodeId;

    private String claimedAt;

    private LocalDateTime startedAt;

    private LocalDateTime completedAt;

    private LocalDateTime lastUpdateAt;

    private long batchItemsProcessed = -1;

    private int retryCount = 0;

    @Indexed
    private ExecStatus status = ExecStatus.NEW;

    private CheckPointState checkPointState;

    public static StepRecord create(String jobRecordId, String jobName, String stepName, Arguments jobArguments){
        val sr = new StepRecord();
        sr.jobRecordId = jobRecordId;
        sr.jobName = jobName;
        sr.stepName = stepName;
        sr.jobArguments = jobArguments;
        return sr;
    }

}
