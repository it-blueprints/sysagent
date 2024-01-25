package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;

import java.util.List;

public interface Step {

    void execute(StepContext context);

    List<Arguments> getPartitionArgumentsList(Arguments jobArguments);

    //-----------------------------------
    default String getName(){
        return this.getClass().getName();
    }

}
