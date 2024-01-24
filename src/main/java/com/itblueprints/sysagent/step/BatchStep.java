package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.Utils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

@Slf4j
public abstract class BatchStep<IN, OUT> implements Step {

    @Setter
    private ThreadManager threadManager;

    //-------------------------------------------------------------------
    @Override
    public void execute(StepContext context) {

        int lotSize = threadManager.getWorkerTaskQueuSize();
        preProcess(context);

        long itemsProcessed = 0;
        int pgNum = 0;
        int totalPages = 0;

        var pageRequest = PageRequest.of(pgNum, threadManager.getBatchPageSize());
        var pgIn = readPageOfItems(pageRequest, context);
        while(true) {

            if(totalPages == 0) {
                totalPages = pgIn.getTotalPages();
                log.debug("Total pages = "+totalPages+", pageSize = "+threadManager.getBatchPageSize());
            }

            var futures = new ArrayList<Future<OUT>>();
            val results = new ArrayList<OUT>();
            for(val item : pgIn){
                val future = threadManager.getExecutor().submit(() -> processItem(item, context));
                futures.add(future);
                if(futures.size() == lotSize){
                    results.addAll(getFutureResults(futures));
                    futures.clear();
                }
            }
            results.addAll(getFutureResults(futures));
            val pgOut = new PageImpl<>(results, pageRequest, results.size());
            writePageOfItems(pgOut, context);
            itemsProcessed += results.size();

            if(isSelectionFixed()){
                pgNum++;
                if(pgNum == totalPages) break;
            }

            pageRequest = PageRequest.of(pgNum, threadManager.getBatchPageSize());
            pgIn = readPageOfItems(pageRequest, context);

            if(pgIn.getTotalElements() == 0) break;

        }

        log.debug("Num items processed = "+itemsProcessed);
        context.setItemsProcessed(itemsProcessed);

        postProcess(context);
    }

    //-------------------------------------------------------------
    private List<OUT> getFutureResults(List<Future<OUT>> futures){
        val results = new ArrayList<OUT>();
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
        return results;
    }

    //----------------------------------------------------
    public abstract void preProcess(StepContext context);

    public abstract Page<IN> readPageOfItems(Pageable pageRequest, StepContext context);

    public abstract OUT processItem(IN item, StepContext context);

    public abstract void writePageOfItems(Page<OUT> page, StepContext context);

    public abstract void postProcess(StepContext context);

    public abstract boolean isSelectionFixed();
}
