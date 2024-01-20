package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.ThreadManager;
import lombok.val;
import org.springframework.data.domain.Page;

public interface BatchStep<IN, OUT> extends Step {

    @Override
    default void execute(StepContext context){
        throw new UnsupportedOperationException();
    }

    default void execute(StepContext context, ThreadManager threadManager){
        try {
            preProcess(context);
            val pg1_in = getPageOfInputItems(0, context);
            val pg1_out = pg1_in.map( itm -> _processItemWrapper(itm, context));
            savePageOfOutputItems(pg1_out, context);

            if(pg1_in.getTotalPages() > 1){
                for(int i = 1; i < pg1_in.getTotalPages(); i++){
                    val pg_in = getPageOfInputItems(i, context);
                    val pg_out = pg_in.map( itm -> _processItemWrapper(itm, context));
                    savePageOfOutputItems(pg_out, context);
                }
            }
            postProcess(context);

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    default OUT _processItemWrapper(IN item, StepContext context){
        try {
            processItem(item, context);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }


    void preProcess(StepContext context) throws Exception;

    Page<IN> getPageOfInputItems(int pageNum, StepContext context) throws Exception;

    OUT processItem(IN item, StepContext context) throws Exception;

    void savePageOfOutputItems(Page<OUT> items, StepContext context) throws Exception;

    void postProcess(StepContext context) throws Exception;
}
