package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.ThreadManager;
import org.springframework.data.domain.Page;

import java.util.List;

public interface BatchStep<IN, OUT> extends Step {

    @Override
    default void execute(StepContext context){
        throw new UnsupportedOperationException();
    }

    default void execute(StepContext context, ThreadManager threadManager){

    }

    void preProcess(StepContext context) throws Exception;

    Page<IN> getPageOfItems(int pageNum, StepContext context) throws Exception;

    OUT processItem(IN item, StepContext context) throws Exception;

    void saveOutputItems(List<OUT> items, StepContext context) throws Exception;

    void postProcess(StepContext context) throws Exception;
}
