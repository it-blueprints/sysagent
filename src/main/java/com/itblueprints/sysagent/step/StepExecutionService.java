package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.ExecStatus;
import com.itblueprints.sysagent.SysAgentException;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.Utils;
import com.itblueprints.sysagent.cluster.ClusterInfo;
import com.itblueprints.sysagent.job.JobExecutionService;
import com.itblueprints.sysagent.repository.RecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

@Component
@RequiredArgsConstructor
@Slf4j
public class StepExecutionService {
    private final JobExecutionService jobExecutionService;
    private final ThreadManager threadManager;
    private final RecordRepository repository;

    //-------------------------------------------------------------
    public void onHeartBeat(ClusterInfo clusterInfo, LocalDateTime now) {

        if(clusterInfo.isBusy) {
            log.debug("Node busy. Not taking on additional work");
            return;
        }

        var stepProcessed = false;
        do {
            stepProcessed = processStepIfAvailable(clusterInfo, now);
        } while(stepProcessed);
    }

    //-------------------------------------------------------------
    boolean processStepIfAvailable(ClusterInfo clusterInfo, LocalDateTime now){
        val stepRec = repository.tryClaimNextStepRecord(clusterInfo.nodeId);
        if (stepRec != null) {
            processStep(stepRec, now);
            threadManager.drainWorkerTaskQueue();
            return true;
        }
        else return false;
    }

    //-------------------------------------------------------------
    void processStep(StepRecord stepRec, LocalDateTime now){
        threadManager.setNodeBusy(true);
        stepRec.setStatus(ExecStatus.RUNNING);
        stepRec.setStartedAt(now);
        repository.save(stepRec);

        val step = jobExecutionService.getStep(stepRec.getJobName(), stepRec.getStepName());
        val ctx = new StepContext();
        ctx.getArguments().add(stepRec.getJobArguments());
        if(stepRec.getPartitionCount() > 0) {
            ctx.getArguments().add(stepRec.getPartitionArguments());
            ctx.setPartitionNum(stepRec.getPartitionNum());
            ctx.setTotalPartitions(stepRec.getPartitionCount());
        }

        log.debug("Executing step '"+stepRec.getStepName() + "' with arguments "+ctx.getArguments());

        try {
            if(step instanceof Batched){
                runBatched((Batched) step, ctx);
            }
            else if(step instanceof SimpleStep){
                ((SimpleStep) step).run(ctx);
            }
        }
        catch (Exception e){
            stepRec.setStatus(ExecStatus.FAILED);
            repository.save(stepRec);
            throw new SysAgentException("Batch step failed - "+step.getName(), e);
        }
        finally {
            threadManager.setNodeBusy(false);
        }
        stepRec.setBatchItemsProcessed(ctx.getItemsProcessed());
        stepRec.setStatus(ExecStatus.COMPLETE);
        stepRec.setCompletedAt(LocalDateTime.now());
        repository.save(stepRec);

        threadManager.setNodeBusy(false);
    }

    //----------------------------------------------------------------------
    <IN, OUT> void runBatched(Batched<IN, OUT> batchStep, StepContext context){
        //This is the number future submissions allowed at a time
        int lotSize = threadManager.getTaskQueueSize();

        //Call pre process()
        batchStep.onStart(context);

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
        batchStep.onComplete(context);
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
}
