package com.itblueprints.sysagent.job;

public interface Job {

    JobPipeline getPipeline();

    default void onStart(JobArguments jobArguments){};

    default void onComplete(JobArguments jobArguments){};

    //-----------------------------------
    default String getName(){
        return this.getClass().getName();
    }

}
