package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.job.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

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
                executeBatchStep((BatchStep) step, ctx);
            }
            else step.execute(ctx);
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

    //----------------------------------------------------------------------
    private <IN, OUT> void executeBatchStep(BatchStep<IN, OUT> batchStep, StepContext context){
        //This is the number future submissions allowed at a time
        int lotSize = threadManager.getWorkerTaskQueuSize();

        //Call pre process()
        batchStep.preProcess(context);

        long itemsProcessed = 0;
        int pgNum = 0;
        int totalPages = -1;

        //Make the first page request i.e. page num = 0
        var pageRequest = PageRequest.of(pgNum, threadManager.getBatchPageSize());
        var pgIn = batchStep.readPageOfItems(pageRequest, context);
        while(true) {

            //If total pages not initialised, get the value from the first page response
            if(totalPages == -1) {
                totalPages = pgIn.getTotalPages();
                log.debug("Total pages = "+totalPages+", pageSize = "+threadManager.getBatchPageSize());
            }

            var futures = new ArrayList<Future<OUT>>();
            val results = new ArrayList<OUT>();

            //For each input item in the page
            for(val item : pgIn){
                //Submit a future computation by calling process item()
                val future = threadManager.getExecutor().submit(() -> batchStep.processItem(item, context));
                futures.add(future);

                //If futures submission limit reached
                if(futures.size() == lotSize){
                    //Wait for futures to finish and collect the results
                    compileResults(futures, results);
                    futures.clear();
                }
            }
            //Collect the remaining results
            compileResults(futures, results);

            //Put the results in a page and call  write page ()
            val pgOut = new PageImpl<>(results, pageRequest, results.size());
            batchStep.writePageOfItems(pgOut, context);
            itemsProcessed += results.size();

            //If this a fixed selection, then advance the page num
            if(batchStep.isSelectionFixed()){
                pgNum++;
                //End the loop if this was the last page
                if(pgNum == totalPages) break;
            }
            //else i.e. in case of dynamic selection, page num is left at 0

            //Make the next page request
            pageRequest = PageRequest.of(pgNum, threadManager.getBatchPageSize());
            pgIn = batchStep.readPageOfItems(pageRequest, context);

            //End the eloop if no more items to process
            if(pgIn.getTotalElements() == 0) break;

        }

        log.debug("Num items processed = "+itemsProcessed);
        context.setItemsProcessed(itemsProcessed);

        //Call post process()
        batchStep.postProcess(context);
    }

    //-----------------------------------------------------------------------
    private <T> void compileResults(List<Future<T>> futures, List<T> results){

        try {
            boolean done = false;
            do {
                val notDoneCount = futures.stream().filter(f -> !f.isDone()).count();
                if(notDoneCount > 0) Utils.sleepFor(notDoneCount*20); //Wait 20 ms per item not done
                else done = true;
            } while (!done);
            for (val future : futures) {
                results.add(future.get());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
