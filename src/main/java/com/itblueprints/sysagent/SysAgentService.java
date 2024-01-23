package com.itblueprints.sysagent;

import com.itblueprints.sysagent.cluster.ClusterService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import javax.management.Query;

@Component
@RequiredArgsConstructor
public class SysAgentService {

    private final MongoTemplate mongoTemplate;
    private final ClusterService clusterService;

    public void deleteClusterData() {
        mongoTemplate.remove(new Query(), "jobRecord");
        mongoTemplate.remove(new Query(), "jobScheduleRecord");
        mongoTemplate.remove(new Query(), "nodeState");
        mongoTemplate.remove(new Query(), "stepRecord");
    }
}
