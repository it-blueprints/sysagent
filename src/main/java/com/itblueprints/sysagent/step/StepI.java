package com.itblueprints.sysagent.step;

public interface StepI {

    default String getName(){
        return this.getClass().getName();
    }

}
