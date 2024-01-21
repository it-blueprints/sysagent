package com.itblueprints.sysagent;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

@Component
@RequiredArgsConstructor
public class ThreadManager {

    private final Config config;

    @Getter
    private ExecutorService executor;

    private ExecutorCompletionService<Boolean> completionService;

    //-----------------------------
    @Getter
    private int batchPageSize;

    //--------------------------------------
    public void submit(Runnable task){
        executor.submit(task);
    }

    //-------------------------------------------
    public void submit(Callable<Boolean> batchTask){
        completionService.submit(batchTask);
    }

    //-----------------------------------------
    @PostConstruct
    void init(){
        batchPageSize = config.getBatchChunkSize();

        executor  = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                1000L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(batchPageSize));

    }
}
