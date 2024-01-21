package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.ThreadManager;
import lombok.val;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

public interface BatchStep<T> extends Step {

    @Override
    default void execute(StepContext context){
        throw new UnsupportedOperationException();
    }

    default void execute(StepContext context, ThreadManager threadManager){
        try {
            preProcess(context);
            int pgNum = 0;
            int totalPages = 0;
            do {
                val pageRequest = PageRequest.of(pgNum, threadManager.getBatchPageSize());
                val pg_in = getPageOfItems(pageRequest, context);
                if(totalPages == 0) totalPages = pg_in.getTotalPages();
                int count = 0;
                for(val item : pg_in){
                    threadManager.submit(() -> processItem(item, context));
                    count++;
                }
                int successCount = threadManager.waitTillComplete(count);
                pgNum++;
            } while (pgNum < totalPages);
            postProcess(context);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }


    void preProcess(StepContext context) throws Exception;

    Page<T> getPageOfItems(Pageable pageRequest, StepContext context) throws Exception;

    boolean processItem(T item, StepContext context) throws Exception;

    void postProcess(StepContext context) throws Exception;
}
