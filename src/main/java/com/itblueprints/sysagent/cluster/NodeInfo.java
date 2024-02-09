package com.itblueprints.sysagent.cluster;

import lombok.ToString;

import java.time.LocalDateTime;
import java.util.List;

/**
 * A transient data structure that holds the current state of this node
 * as welll as the cluster it is running in
 */
@ToString
public class NodeInfo {
    /**
     * The time now
     */
    public LocalDateTime timeNow;

    /**
     * The id of this node. This is the id of the corresponding NodeRecord in the DB
     */
    public String nodeId;

    /**
     * If this node is the manager node
     */
    public boolean isManager;

    /**
     * If this node is busy i.e. there is a Step that is
     * currently being processed
     */
    public boolean isNodeBusy;

    /**
     * The list of node ids for nodes that are dead i.e.
     * faield to update the life lease
     */
    public List<String> deadNodeIds;
}
