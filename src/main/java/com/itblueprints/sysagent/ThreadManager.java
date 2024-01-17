package com.itblueprints.sysagent;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Component
public class ThreadManager {

    private final static int MAX_QUEUE_SIZE = 100;
    private final static int MAX_POOL_SIZE = 5;


    private final ExecutorService executor =
            new ThreadPoolExecutor(1, MAX_POOL_SIZE, 1000L,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(MAX_QUEUE_SIZE));

    private List<Future> taskHandles = new ArrayList<>();

    public void submit(Runnable task){
        taskHandles.add(executor.submit(task));
    }
}
