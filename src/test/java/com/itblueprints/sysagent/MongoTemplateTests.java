package com.itblueprints.sysagent;

import com.itblueprints.sysagent.cluster.NodeRecord;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MongoTemplateTests {

    @Test
    void testIdConstraint(){

        val repository = TestUtils.getRecordRepository();

        //Node 1 writes the manager record
        val nodeRec1 = new NodeRecord();
        nodeRec1.setId("M");
        nodeRec1.setManagerNodeId("N1");
        repository.save(nodeRec1);

        //Node 2 writes the manager record.
        //But this would fail silently and the record will not be updated
        //due to the constraint on the id field
        val nodeRec2 = new NodeRecord();
        nodeRec1.setId("M");
        nodeRec1.setManagerNodeId("N2");
        repository.save(nodeRec2);

        //So when we get the manager record ...
        val mgrNr = repository.getManagerNodeRecord();
        //...it should still be node 1
        assertEquals("N1", mgrNr.getManagerNodeId());

    }
}
