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

package com.itblueprints.sysagent.internal;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@RequiredArgsConstructor
@Slf4j
public class ThreadManager {

    private final Config config;

    @Getter
    private ExecutorService executor;

    private LinkedBlockingQueue<Runnable> taskQueue;

    //-----------------------------
    @Getter
    private int batchPageSize;

    @Getter
    private int taskQueueSize;

    //-----------------------------------------------------
    private static final int TASK_Q_CAPACITY_FACTOR = 10;

    @PostConstruct
    public void init(){

        val numThreads = config.getWorkerThreads();
        taskQueueSize = numThreads * TASK_Q_CAPACITY_FACTOR;
        taskQueue = new LinkedBlockingQueue<>(taskQueueSize + 10);
        batchPageSize = config.getBatchPageSize();

        executor = new ThreadPoolExecutor(
                numThreads,
                numThreads,
                1000L,
                TimeUnit.MILLISECONDS,
                taskQueue);

        log.debug("ThreadManager initialised. Available threads="+numThreads);

    }

    //---------------------------------------
    public void drainWorkerTaskQueue(){
        while(taskQueue.size() > 0) Utils.sleepFor(100);
    }

    //---------------------------------------------------
    private AtomicBoolean isNodeBusy = new AtomicBoolean(false);
    public boolean isNodeBusy(){return isNodeBusy.get();}
    public void setNodeBusy(boolean busy){ isNodeBusy.set(busy);}

}
