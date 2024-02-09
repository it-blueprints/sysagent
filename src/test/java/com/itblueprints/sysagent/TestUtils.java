package com.itblueprints.sysagent;

import com.itblueprints.sysagent.repository.MongoRecordRepository;
import com.itblueprints.sysagent.repository.RecordRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import lombok.val;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUtils {
    //------------------------------------------------------------
    public static <T> void assertTrueForAll(List<T> items, Predicate<T> pred){
        val count = items.stream().filter(pred).count();
        assertEquals(items.size(), count);
    }

    //------------------------------------------------------------
    private static MongoClient mongoClient;
    public static RecordRepository getRecordRepository(Class clazz){
        if(mongoClient == null) {
            val mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:latest"));
            mongoDBContainer.start();
            val host = mongoDBContainer.getHost();
            val port = mongoDBContainer.getMappedPort(27017);
            mongoClient = MongoClients.create("mongodb://" + host + ":" + port);
        }
        val mongoTemplate = new MongoTemplate(mongoClient, clazz.getSimpleName());
        return new MongoRecordRepository(mongoTemplate);
    }
}
