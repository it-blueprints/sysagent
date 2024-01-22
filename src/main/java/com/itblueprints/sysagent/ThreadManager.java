package com.itblueprints.sysagent;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;
import java.util.function.Supplier;

@Component
@RequiredArgsConstructor
public class ThreadManager {

    private final Config config;

    @Getter
    private ExecutorService executor;

    //-----------------------------
    @Getter
    private int batchChunkSize;
    @Getter
    private int batchQueueSize;

    //--------------------------------------
    public CompletableFuture submitRunnable(Runnable task){
        return CompletableFuture.runAsync(task, executor);
    }

    //--------------------------------------
    public <T> CompletableFuture submitSupplier(Supplier<T> task){
        return CompletableFuture.supplyAsync(task, executor);
    }

    //-----------------------------------------
    @PostConstruct
    public void init(){
        batchChunkSize = config.getBatchChunkSize();
        batchQueueSize = config.getBatchQueueSize();
        val numWorkerThreads = Runtime.getRuntime().availableProcessors();

        executor  = new ThreadPoolExecutor(
                numWorkerThreads,
                numWorkerThreads,
                1000L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(numWorkerThreads*10));

    }
}
