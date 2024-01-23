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

@Component
@RequiredArgsConstructor
public class ThreadManager {

    private final Config config;

    @Getter
    private ExecutorService executor;

    //-----------------------------
    @Getter
    private int batchChunkSize;

    //-----------------------------------------
    @PostConstruct
    public void init(){
        batchChunkSize = config.getBatchChunkSize();
        val numWorkerThreads = Runtime.getRuntime().availableProcessors();

        executor  = new ThreadPoolExecutor(
                numWorkerThreads,
                numWorkerThreads,
                1000L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(numWorkerThreads*10));

    }
}
