package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.MapData;
import lombok.val;

import java.util.Map;

public class JobArguments extends MapData {

    @Override
    public String toString() {
        return super.toString();
    }

    //---------------------------------------------------
    public static JobArguments from(Map<String, Object> map){
        val args = new JobArguments();
        args.loadFrom(map);
        return args;
    }

}
