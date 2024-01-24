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

    //-------------------------------------------------------------
    public void onHeartBeat(NodeInfo nodeInfo, LocalDateTime now) {

        if(nodeInfo.isBusy) {
            log.debug("Node busy. Not taking on additional work");
            return;
        }

        log.debug("Looking for next step to execute");
        val stepRec = getNextStepToProcess(nodeInfo.thisNodeId);
        if(stepRec != null) processStep(stepRec, now);
    }

    //-------------------------------------------------------------
    private void processStep(StepRecord stepRec, LocalDateTime now){
        threadManager.setNodeBusy(true);
        stepRec.setStatus(StepRecord.Status.Executing);
        stepRec.setStartedAt(now);
        mongoTemplate.save(stepRec);

        val step = jobService.getStep(stepRec.getJobName(), stepRec.getStepName());
        val ctx = new StepContext();
        ctx.getArguments().add(stepRec.getJobArguments());
        if(stepRec.getPartitionCount() > 0) {
            ctx.getArguments().add(stepRec.getPartitionArguments());
            ctx.setPartitionNum(stepRec.getPartitionNum());
            ctx.setTotalPartitions(stepRec.getPartitionCount());
        }

        log.debug("Executing step '"+stepRec.getStepName() + "' with arguments "+ctx.getArguments());

        try {
            if(step instanceof BatchStep){
                ((BatchStep) step).setThreadManager(threadManager);
            }
            step.execute(ctx);
        }
        catch (Exception e){
            e.printStackTrace();
            throw new SysAgentException("Batch step "+step.getName()+" failed", e);
        }
        stepRec.setBatchItemsProcessed(ctx.getItemsProcessed());
        stepRec.setStatus(StepRecord.Status.Completed);
        stepRec.setCompletedAt(LocalDateTime.now());
        mongoTemplate.save(stepRec);

        threadManager.setNodeBusy(false);
    }

    //----------------------------------------------
    private StepRecord getNextStepToProcess(String nodeId){
        //Read leader record with lock
        val query = new Query();
        query.addCriteria(
                Criteria.where("claimed").is(false)
        );

        val update = new Update();
        update.set("claimed", true);
        update.set("nodeId", nodeId);
        val lockedStepRec = mongoTemplate.findAndModify(query, update, StepRecord.class);
        if(lockedStepRec!=null) { //because this is the pre update value
            lockedStepRec.setClaimed(true);
            lockedStepRec.setNodeId(nodeId);
        }
        return lockedStepRec;
    }
}
