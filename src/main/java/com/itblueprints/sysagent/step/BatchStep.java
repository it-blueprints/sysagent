package com.itblueprints.sysagent.step;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;


public interface BatchStep<IN, OUT> extends Step {

    @Override
    default void execute(StepContext context) {
        throw new UnsupportedOperationException();
    }

    void preProcess(StepContext context);

    Page<IN> readPageOfItems(Pageable pageRequest, StepContext context);

    OUT processItem(IN item, StepContext context);

    void writePageOfItems(Page<OUT> page, StepContext context);

    void postProcess(StepContext context);

    boolean isSelectionFixed();
}
