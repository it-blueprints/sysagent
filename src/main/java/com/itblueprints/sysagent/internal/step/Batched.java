package com.itblueprints.sysagent.internal.step;

import com.itblueprints.sysagent.step.BatchStepContext;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;


public interface Batched<IN, OUT> {

    default void onStart(BatchStepContext context){}

    Page<IN> readPageOfItems(Pageable pageRequest, BatchStepContext context);

    OUT processItem(IN item, BatchStepContext context);

    void writePageOfItems(Page<OUT> page, BatchStepContext context);

    default void onComplete(BatchStepContext context){}

    /*
    This field indicates if the query to fetch items, returns the same result even if items have
    been processed. In other words, there is either no flag set on an item to mark it as processed
    or a flag is set, but it is not used in the query to select items. In each case, the batch system
    uses different ways to fetch the data. Note that in most cases, this should be false, as we would
    like to mark an item as processed and then not fetch it for processing again. However sometimes
    it may be set too true when we want to process all records in each run.
     */
    default boolean isSelectionFixed(){
        return false;
    }
}
