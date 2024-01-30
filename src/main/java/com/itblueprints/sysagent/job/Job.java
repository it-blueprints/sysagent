package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;

public interface Job {

    JobPipeline getPipeline();

    void onStarted(Arguments jobArguments);

    void onCompleted();

    //-----------------------------------
    default String getName(){
        return this.getClass().getName();
    }

}
