package com.itblueprints.sysagent;

import com.itblueprints.sysagent.cluster.NodeRecord;
import com.itblueprints.sysagent.repository.MongoRecordRepository;
import com.mongodb.client.MongoClients;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MongoTemplateTests {

    static MongoRecordRepository repo;

    @BeforeAll
    static void init() {
        val mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:latest"));
        mongoDBContainer.start();
        val host = mongoDBContainer.getHost();
        val port = mongoDBContainer.getMappedPort(27017);
        val mongoClient = MongoClients.create("mongodb://"+host+":"+port);
        val mongoTemplate = new MongoTemplate(mongoClient, "test");
        repo = new MongoRecordRepository(mongoTemplate);
    }

    @Test
    void testIdConstraint(){

        //Node 1 writes the manager record
        val nodeRec1 = new NodeRecord();
        nodeRec1.setId("M");
        nodeRec1.setManagerNodeId("N1");
        repo.save(nodeRec1);

        //Node 2 writes the manager record.
        //But this would fail silently and the record will not be updated
        //due to the constraint on the id field
        val nodeRec2 = new NodeRecord();
        nodeRec1.setId("M");
        nodeRec1.setManagerNodeId("N2");
        repo.save(nodeRec2);

        //So when we get the manager record ...
        val mgrNr = repo.getManagerNodeRecord();
        //...it should still be node 1
        assertEquals("N1", mgrNr.getManagerNodeId());

    }
}
