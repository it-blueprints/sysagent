package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.ThreadManager;
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

        preProcess(context);

        int pgNum = 0;
        int totalPages = 0;
        do {
            val pageRequest = PageRequest.of(pgNum, threadManager.getBatchChunkSize());
            val pgIn = readChunkOfItems(pageRequest, context);
            if(totalPages == 0) {
                totalPages = pgIn.getTotalPages();
                log.debug("Total chunks = "+totalPages);
            }

            var futures = new ArrayList<Future<OUT>>();
            List<OUT> results = new ArrayList<>();
            for(val item : pgIn){
                val future = threadManager.getExecutor().submit(() -> processItem(item, context));
                //val future = CompletableFuture.supplyAsync(() -> processItem(item, context));

                futures.add(future);
                if(futures.size() == threadManager.getBatchQueueSize()){
                    results.addAll(getFutureResults(futures));
                    futures.clear();
                }
            }
            results.addAll(getFutureResults(futures));
            val pgOut = new PageImpl<>(results, pageRequest, results.size());
            writeChunkOfItems(pgOut, context);
            pgNum++;
        } while (pgNum < totalPages);

        postProcess(context);

    }

    //-----------------------------------------------------------------------
    private List<OUT> getFutureResults(List<Future<OUT>> futures){
        val results = new ArrayList<OUT>();
        try {
            for (val future : futures) {
                results.add(future.get());
            }
        }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return results;
    }

    //----------------------------------------------------
    public abstract void preProcess(StepContext context);

    public abstract Page<IN> readChunkOfItems(Pageable pageRequest, StepContext context);

    public abstract OUT processItem(IN item, StepContext context);

    public abstract void writeChunkOfItems(Page<OUT> page, StepContext context);

    public abstract void postProcess(StepContext context);
}
