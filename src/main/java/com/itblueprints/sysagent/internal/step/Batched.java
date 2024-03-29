/*
 * Copyright 2023-2024 the original author or authors.
 *
 * Licensed under the Apache Software License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.itblueprints.sysagent.internal.step;

import com.itblueprints.sysagent.step.StepContext;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;


public interface Batched<IN, OUT> {

    default void onStart(StepContext context){}

    Page<IN> readPageOfItems(Pageable pageRequest, StepContext context);

    OUT processItem(IN item, StepContext context);

    void writePageOfItems(Page<OUT> page, StepContext context);

    default void onComplete(StepContext context){}

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
