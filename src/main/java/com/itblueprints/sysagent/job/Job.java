package com.itblueprints.sysagent.job;

public interface Job {

    JobPipeline getPipeline();

    //-----------------------------------
    default String getName(){
        return this.getClass().getName();
    }

}
