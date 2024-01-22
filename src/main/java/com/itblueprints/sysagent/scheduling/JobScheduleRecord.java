package com.itblueprints.sysagent.scheduling;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Getter
@Setter
@Document
public class JobScheduleRecord {
    @Id
    private String jobName;

    private LocalDateTime lastRunAt = LocalDateTime.MIN;

}
