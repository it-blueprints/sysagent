package com.itblueprints.sysagent.internal.cluster;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * A persistent data structure that records the state of a node. Actual subclasses are NodeRecord and
 * ManagerNodeRecord. Each node in the cluster has its own NodeRecord that holds its life lease state.
 * Plus there is one ManagerNodeRecord which holds lease information about the manager node. So if there
 * are 3 nodes in the cluster there will be 3 + 1 = 4 records in the DB at starup.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="type", visible=true)
@JsonSubTypes({
        @JsonSubTypes.Type(value= NodeRecord.class, name= "node"),
        @JsonSubTypes.Type(value= ManagerNodeRecord.class, name= "manager")
})

@Document
@Getter
public abstract class BaseNodeRecord {

    /**
     * The DB assigned id. This is used as the unique id
     * of the node within the cluster
     */
    @Id
    @Setter
    private String id;
}
