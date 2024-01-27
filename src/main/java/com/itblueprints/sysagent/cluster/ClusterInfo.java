package com.itblueprints.sysagent.cluster;

import lombok.ToString;

import java.time.LocalDateTime;
import java.util.List;

@ToString
public class ClusterInfo {
    public LocalDateTime timeNow;
    public String nodeId;
    public boolean isManager;
    public boolean isBusy;
    public List<String> deadNodeIds;
}
