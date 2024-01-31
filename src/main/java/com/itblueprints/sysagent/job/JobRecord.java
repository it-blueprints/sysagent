package com.itblueprints.sysagent.job;

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
public class JobRecord {

    @Id
    private String id;

    private String jobName;

    @Indexed
    private ExecStatus status;

    private Arguments jobArguments;

    private LocalDateTime startedAt;

    private LocalDateTime completedAt;

    private LocalDateTime lastUpdateAt;

    private String currentStepName;

    private int partitionCount = 0;

    private int partitionsCompletedCount = 0;

    private int retryCount = 0;

    //------------------------------------------
    public static JobRecord create(String jobName, Arguments jobArguments, LocalDateTime startedAt) {
        val jr = new JobRecord();
        jr.jobName = jobName;
        jr.status = ExecStatus.RUNNING;
        jr.jobArguments = jobArguments;
        jr.startedAt = startedAt;
        jr.lastUpdateAt = startedAt;
        return jr;
    }
}
