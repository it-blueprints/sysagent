package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.ThreadManager;
import lombok.val;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

public abstract class BatchStep<IN, OUT> implements Step {

    @Override
    public void execute(StepContext context){
        throw new UnsupportedOperationException();
    }

    private ExecutorCompletionService<OUT> completionService;

    public void execute(StepContext context, ThreadManager threadManager){

        completionService = new ExecutorCompletionService<>(threadManager.getExecutor());

        try {
            preProcess(context);
            int pgNum = 0;
            int totalPages = 0;
            do {
                val pageRequest = PageRequest.of(pgNum, threadManager.getBatchPageSize());
                val pg_in = readChunkOfItems(pageRequest, context);
                if(totalPages == 0) totalPages = pg_in.getTotalPages();
                int count = 0;
                for(val item : pg_in){
                    threadManager.submit(() -> {
                        try {
                            processItem(item, context);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                    count++;
                }

                List<OUT> results = new ArrayList<>();
                for(int i=0; i < count; i++){
                    try {
                        val result = completionService.take().get();
                        results.add(result);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                writeChunkOfItems(results, context);
                pgNum++;
            } while (pgNum < totalPages);
            postProcess(context);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }


    public abstract void preProcess(StepContext context) throws Exception;

    public abstract Page<IN> readChunkOfItems(Pageable pageRequest, StepContext context) throws Exception;

    public abstract OUT processItem(IN item, StepContext context) throws Exception;

    public abstract void writeChunkOfItems(Collection<OUT> items, StepContext context) throws Exception;

    public abstract void postProcess(StepContext context) throws Exception;
}
