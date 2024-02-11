package com.itblueprints.sysagent.internal.cluster;

import lombok.Getter;
import lombok.Setter;

/**
 * NodeRecord holding details for a running node. This record is used to check
 * if the node is still alive.When a node dies the lifeLeaseTill value is not
 * extended, thus indicating that the node is dead. The manager node then comes
 * along and clears out those NodeRecords
 */
@Getter
@Setter
public class NodeRecord extends BaseNodeRecord {

    /**
     * When the node started up
     */
    private long startedAt;

    /**
     * This is the time upto which this node should be considered to be alive. If the
     * current time is greater than this then the node is considered dead. With each
     * heartbeat a node extends the value of this field.
     */
    private long lifeLeaseTill;
}
