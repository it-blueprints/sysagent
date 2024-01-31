package com.itblueprints.sysagent.step;

public interface Step {

    default String getName(){
        return this.getClass().getName();
    }

}
