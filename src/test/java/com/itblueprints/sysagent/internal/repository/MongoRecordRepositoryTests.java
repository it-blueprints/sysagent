package com.itblueprints.sysagent.internal.repository;

import com.itblueprints.sysagent.TestUtils;
import com.itblueprints.sysagent.internal.cluster.ManagerNodeRecord;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MongoRecordRepositoryTests {

    @Test
    void testIdConstraint(){

        val repository = TestUtils.getRecordRepository(this.getClass());

        //Node 1 writes the manager record
        val nodeRec1 = new ManagerNodeRecord();
        nodeRec1.setManagerNodeId("N1");
        repository.save(nodeRec1);

        var mgrNr = repository.getManagerNodeRecord();

        //Node 2 writes the manager record.
        //But this would fail silently and the record will not be updated
        //due to the constraint on the id field
        val nodeRec2 = new ManagerNodeRecord();
        nodeRec2.setManagerNodeId("N2");
        repository.save(nodeRec2);

        //So when we get the manager record ...
        mgrNr = repository.getManagerNodeRecord();
        //...it should still be node 1
        assertEquals("N1", mgrNr.getManagerNodeId());

    }
}
