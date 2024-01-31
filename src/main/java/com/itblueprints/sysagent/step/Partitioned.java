package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import lombok.val;

import java.util.List;

public interface Partitioned {

    List<Arguments> getPartitionArgumentsList(Arguments jobArguments);

}
