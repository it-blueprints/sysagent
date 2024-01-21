package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.ThreadManager;
import lombok.Setter;
import lombok.val;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;

public abstract class BatchStep<IN, OUT> implements Step {

    @Setter
    private ThreadManager threadManager;

    private ExecutorCompletionService<OUT> completionService;

    //-------------------------------------------------------------------
    @Override
    public void execute(StepContext context) {

        completionService = new ExecutorCompletionService<>(threadManager.getExecutor());

        preProcess(context);

        int pgNum = 0;
        int totalPages = 0;
        do {
            val pageRequest = PageRequest.of(pgNum, threadManager.getBatchPageSize());
            val pg_in = readChunkOfItems(pageRequest, context);
            if(totalPages == 0) totalPages = pg_in.getTotalPages();
            int count = 0;
            for(val item : pg_in){
                threadManager.submit(() -> processItem(item, context));
                count++;
            }

            List<OUT> results = new ArrayList<>();
            for(int i=0; i < count; i++){
                try {
                    val result = completionService.take().get();
                    results.add(result);
                }
                catch (Exception e){
                    throw new RuntimeException(e);
                }
            }
            writeChunkOfItems(results, context);
            pgNum++;
        } while (pgNum < totalPages);

        postProcess(context);

    }

    //----------------------------------------------------
    public abstract void preProcess(StepContext context);

    public abstract Page<IN> readChunkOfItems(Pageable pageRequest, StepContext context);

    public abstract OUT processItem(IN item, StepContext context);

    public abstract void writeChunkOfItems(Collection<OUT> items, StepContext context);

    public abstract void postProcess(StepContext context);
}
