package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.job.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class StepService {
    private final MongoTemplate mongoTemplate;
    private final JobService jobService;
    private final ThreadManager threadManager;

    //----------------------------------------------
    public void onHeartBeat(NodeInfo nodeInfo) {

        log.debug("Loooking for next step to execute");
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

            log.debug("Executing step '"+stepRec.getStepName() + "' with arguments "+ctx.getArguments());

            try {
                if(step instanceof BatchStep){
                    ((BatchStep) step).setThreadManager(threadManager);
                }
                val future = threadManager.submit(() -> step.execute(ctx));
                future.thenRun(() -> {
                    stepRec.setStatus(StepRecord.Status.Completed);
                    stepRec.setCompletedAt(LocalDateTime.now());
                    mongoTemplate.save(stepRec);
                });
            }
            catch (Exception e){
                e.printStackTrace();
                throw new SysAgentException("Batch step "+step.getName()+" failed", e);
            }
        }
    }

    //----------------------------------------------
    private StepRecord getNextStepToWorkOn(String nodeId){
        //Read leader record with lock
        val query = new Query();
        query.addCriteria(
                Criteria.where("claimed").is(false)
        );

        val update = new Update();
        update.set("claimed", true);
        update.set("nodeId", nodeId);
        val lockedStepRec = mongoTemplate.findAndModify(query, update, StepRecord.class);
        return lockedStepRec;
    }
}
