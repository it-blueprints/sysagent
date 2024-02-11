package com.itblueprints.sysagent.cluster;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.index.Indexed;

/**
 * NodeRecord holding details about the manager node
 */
@Getter
@Setter
public class ManagerNodeRecord extends BaseNodeRecord {

    /**
     * Indicates that this is the manager NodeRecord. Having the unique index ensures
     * that only one ManagerNodeRecord exists in the DB. If there is any attempt to
     * create a new instance (perhaps by another node), then it fails at the DB level.
     */
    @Indexed(unique = true)
    protected final boolean isManagerNodeRecord = true;

    /**
     * The id of the node that is the current manager
     */
    private String managerNodeId;

    /**
     * The time when that node became the manager
     */
    private long managerSince;

    /**
     * The time till when that node will remain manager. The manager extends this
     * with every heartbeat. However, if it fails to do so (because it died) then
     * another node becomes the new manager
     */
    private long managerLeaseTill;

    /**
     * When a node tries to become a manager the NodeRecord is locked using this
     * field just so that no other node gets hold of the record and becomes manager
     */
    private boolean locked;
}
