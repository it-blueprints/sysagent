package com.itblueprints.sysagent;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Component
@RequiredArgsConstructor
public class ThreadManager {

    private final Config config;

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

    //-------------------------------------------
    public int waitTillComplete(int tasksSubmitted){
        int successCount = 0;
        for(int i=0; i < tasksSubmitted; i++){
            try {
                val ok = completionService.take().get();
                if(ok) successCount ++;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return successCount;
    }

    //-----------------------------------------
    @PostConstruct
    void init(){
        batchPageSize = config.getBatchPageSize();

        executor  = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                1000L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(batchPageSize));

        completionService = new ExecutorCompletionService<>(executor);

    }
}
