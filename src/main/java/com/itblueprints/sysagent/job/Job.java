package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;

public interface Job {

    JobPipeline getPipeline();

    void onStart(Arguments jobArguments);

    void onComplete(Arguments jobArguments);

    //-----------------------------------
    default String getName(){
        return this.getClass().getName();
    }

}
