package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.SystemAgentException;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.job.JobService;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class StepService {
    private final MongoTemplate mongoTemplate;
    private final JobService jobService;
    private final ThreadManager threadManager;

    //----------------------------------------------
    public void onHeartBeat(NodeInfo nodeInfo) {
        val stepRec = getNextStepToWorkOn(nodeInfo.thisNodeId);
        if(stepRec != null) {
            stepRec.setStatus(StepRecord.Status.Executing);
            stepRec.setStartedAt(LocalDateTime.now());

            val step = jobService.getStep(stepRec.getJobName(), stepRec.getStepName());
            val ctx = new StepContext();
            ctx.getArguments().add(stepRec.getJobArguments());
            if(stepRec.getTotalPartitions() > 0) {
                ctx.getArguments().add(stepRec.getPartitionArguments());
                ctx.setPartitionNum(stepRec.getPartitionNum());
                ctx.setTotalPartitions(stepRec.getTotalPartitions());
            }

            if(step instanceof BatchStep<?,?>){
                val batchStep = (BatchStep<?,?>) step;
                batchStep.execute(ctx, threadManager);
            }
            else {
                threadManager.submit(() -> {
                    try {
                        step.execute(ctx);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new SystemAgentException("Error running step job " + step.getName(), e);
                    }
                });
            }

        }
    }

    //----------------------------------------------
    private StepRecord getNextStepToWorkOn(String nodeId){
        //Read leader record with lock
        val query = new Query();
        query.addCriteria(Criteria
                .where("claimed").is(false));

        val update = new Update();
        update.set("claimed", true);
        update.set("nodeId", nodeId);
        val lockedStepRec = mongoTemplate.findAndModify(query, update, StepRecord.class);
        return lockedStepRec;
    }
}
