package com.itblueprints.sysagent.step;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

@Slf4j
public abstract class BatchStep2<IN, OUT> extends BatchStep<IN, OUT> {

    //-------------------------------------------------------------------
    @Override
    public void execute(StepContext context) {

        preProcess(context);

        int lotSize = threadManager.getWorkerTaskQueuSize();
        long itemsProcessed = 0;
        var chunkIn = readChunkOfItems(threadManager.getBatchChunkSize(), context);
        while(!chunkIn.isEmpty()) {
            var futures = new ArrayList<Future<OUT>>();
            val results = new ArrayList<OUT>();
            for(val item : chunkIn){
                val future = threadManager.getExecutor().submit(() -> processItem(item, context));
                futures.add(future);
                if(futures.size() == lotSize){
                    results.addAll(getFutureResults(futures));
                    futures.clear();
                }
            }
            results.addAll(getFutureResults(futures));
            writeChunkOfItems(results, context);
            itemsProcessed += results.size();
            chunkIn = readChunkOfItems(threadManager.getBatchChunkSize(), context);
        }
        log.debug("Num items processed = "+itemsProcessed);
        context.setItemsProcessed(itemsProcessed);
        postProcess(context);
    }

    //----------------------------------------------------
    @Override
    public Page<IN> readChunkOfItems(Pageable pageRequest, StepContext context){
        throw new UnsupportedOperationException();
    }
    @Override
    public void writeChunkOfItems(Page<OUT> page, StepContext context){
        throw new UnsupportedOperationException();
    }

    //----------------------------------------------------
    public abstract void preProcess(StepContext context);

    public abstract List<IN> readChunkOfItems(int chunkSize, StepContext context);

    public abstract OUT processItem(IN item, StepContext context);

    public abstract void writeChunkOfItems(List<OUT> chunk, StepContext context);

    public abstract void postProcess(StepContext context);
}
