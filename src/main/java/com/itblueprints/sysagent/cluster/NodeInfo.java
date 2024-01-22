package com.itblueprints.sysagent.cluster;

import lombok.ToString;

import java.time.LocalDateTime;

@ToString
public class NodeInfo {
    public LocalDateTime timeNow;
    public String thisNodeId;
    public boolean isManager;
    public boolean isBusy;
}
