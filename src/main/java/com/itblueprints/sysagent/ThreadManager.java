package com.itblueprints.sysagent;

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
    private int workerTaskQueuSize;

    //-----------------------------------------
    @PostConstruct
    public void init(){

        val workerCapacityFactor = config.getWorkerCapacityFactor();

        val numThreads = config.getWorkerThreads();
        workerTaskQueuSize = numThreads * workerCapacityFactor;
        batchPageSize = workerTaskQueuSize * workerCapacityFactor;

        taskQueue = new LinkedBlockingQueue<>(workerTaskQueuSize + 10);

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
