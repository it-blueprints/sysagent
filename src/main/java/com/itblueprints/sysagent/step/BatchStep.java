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

    Page<IN> getNextPage(int lastPageIndex);

    OUT process(IN item);

    void savePage(List<OUT> page);
}
