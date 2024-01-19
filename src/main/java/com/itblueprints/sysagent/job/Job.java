package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.Arguments;

public interface Job {

    JobPipeline getPipeline();

    void addToJobArguments(Arguments jobArguments);

    //-----------------------------------
    default String getName(){
        return this.getClass().getName();
    }

}
