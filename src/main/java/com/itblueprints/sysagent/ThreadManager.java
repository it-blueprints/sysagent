package com.itblueprints.sysagent;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class ThreadManager {

    @Getter
    private ExecutorService executor;

    private LinkedBlockingQueue<Runnable> taskQueue;

    //-----------------------------
    @Getter
    private int batchChunkSize;

    @Getter
    private int numWorkerThreads;

    @Getter
    private int taskQueuSize;

    //-----------------------------------------
    @PostConstruct
    public void init(){
        numWorkerThreads = Runtime.getRuntime().availableProcessors();
        taskQueuSize = numWorkerThreads * CAPACITY_FACTOR;
        batchChunkSize = taskQueuSize * CAPACITY_FACTOR;

        taskQueue = new LinkedBlockingQueue<>(taskQueuSize+10);

        executor = new ThreadPoolExecutor(
                numWorkerThreads,
                numWorkerThreads,
                1000L,
                TimeUnit.MILLISECONDS,
                taskQueue);

    }

    private static final int CAPACITY_FACTOR = 20;
}
