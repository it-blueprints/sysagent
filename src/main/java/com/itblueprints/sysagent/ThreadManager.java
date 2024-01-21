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

    private final Config config;

    @Getter
    private ExecutorService executor;

    //-----------------------------
    @Getter
    private int batchChunkSize;

    //--------------------------------------
    public void submit(Runnable task){
        executor.submit(task);
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
