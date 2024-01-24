package com.itblueprints.sysagent;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@RequiredArgsConstructor
public class ThreadManager {

    private final Config config;

    @Getter
    private ExecutorService executor;

    private LinkedBlockingQueue<Runnable> taskQueue;

    //-----------------------------
    @Getter
    private int batchChunkSize;

    @Getter
    private int workerTaskQueuSize;

    //-----------------------------------------
    @PostConstruct
    public void init(){

        val workerCapacityFactor = config.getWorkerCapacityFactor();

        val numThreads = Runtime.getRuntime().availableProcessors();
        workerTaskQueuSize = numThreads * workerCapacityFactor;
        batchChunkSize = workerTaskQueuSize * workerCapacityFactor;

        taskQueue = new LinkedBlockingQueue<>(workerTaskQueuSize +JOB_MANAGER_QUEUE_SIZE);

        executor = new ThreadPoolExecutor(
                numThreads,
                numThreads,
                1000L,
                TimeUnit.MILLISECONDS,
                taskQueue);
    }

    private static final int JOB_MANAGER_QUEUE_SIZE = 10;

    //--------------------------
    public AtomicBoolean isNodeBusy;

}
