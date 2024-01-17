package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.ThreadManager;
import org.springframework.data.domain.Page;

import java.util.Optional;

public interface BatchStep<T> extends Step {

    @Override
    default void execute(StepContext context){
        throw new UnsupportedOperationException();
    }

    default void execute(StepContext context, ThreadManager threadManager){

    }

    Optional<Page<T>> getNextPage(Optional<Page<T>> previousPage);

    T process(T item);

    void savePage(Page<T> page);
}
