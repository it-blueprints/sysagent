package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;

public interface Job {

    JobPipeline getPipeline();

    default void onStart(Arguments jobArguments){};

    default void onComplete(Arguments jobArguments){};

    //-----------------------------------
    default String getName(){
        return this.getClass().getName();
    }

}
