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

        //This is the number future submissions allowed at a time
        int lotSize = threadManager.getWorkerTaskQueuSize();

        //Call pre process()
        preProcess(context);

        long itemsProcessed = 0;
        int pgNum = 0;
        int totalPages = -1;

        //Make the first page request i.e. page num = 0
        var pageRequest = PageRequest.of(pgNum, threadManager.getBatchPageSize());
        var pgIn = readPageOfItems(pageRequest, context);
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
                val future = threadManager.getExecutor().submit(() -> processItem(item, context));
                futures.add(future);

                //If futures submission limit reached
                if(futures.size() == lotSize){
                    //Wait for futures to finish and collect the results
                    results.addAll(getFutureResults(futures));
                    futures.clear();
                }
            }
            //Collect the remaining results
            results.addAll(getFutureResults(futures));

            //Put the results in a page and call  write page ()
            val pgOut = new PageImpl<>(results, pageRequest, results.size());
            writePageOfItems(pgOut, context);
            itemsProcessed += results.size();

            //If this a fixed selection, then advance the page num
            if(isSelectionFixed()){
                pgNum++;
                //End the loop if this was the last page
                if(pgNum == totalPages) break;
            }
            //else i.e. in case of dynamic selection, page num is left at 0

            //Make the next page request
            pageRequest = PageRequest.of(pgNum, threadManager.getBatchPageSize());
            pgIn = readPageOfItems(pageRequest, context);

            //End the eloop if no more items to process
            if(pgIn.getTotalElements() == 0) break;

        }

        log.debug("Num items processed = "+itemsProcessed);
        context.setItemsProcessed(itemsProcessed);

        //Call post process()
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
