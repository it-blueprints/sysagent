package com.itblueprints.sysagent;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

@Component
@RequiredArgsConstructor
public class ThreadManager {

    private final Config config;

    @Getter
    private ExecutorService executor;

    //-----------------------------
    @Getter
    private int batchChunkSize;

    //--------------------------------------
    public CompletableFuture submit(Runnable task){
        return CompletableFuture.runAsync(task, executor);
    }

    //-----------------------------------------
    @PostConstruct
    void init(){
        batchChunkSize = config.getBatchChunkSize();

        executor  = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                1000L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(batchChunkSize));

    }
}
