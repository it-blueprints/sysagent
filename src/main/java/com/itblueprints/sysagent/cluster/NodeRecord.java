package com.itblueprints.sysagent.cluster;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@Setter
@Document
public class NodeRecord {

    @Id
    private String id;

    private long startedAt;

    private long aliveTill;

    private String managerNodeId;

    private long managerSince;

    private long managerLeaseTill;

    private boolean locked;

    private boolean initialised;

    public static final String MANAGER_ID = "M";
}
