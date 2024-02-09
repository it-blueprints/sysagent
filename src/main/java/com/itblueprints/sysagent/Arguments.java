package com.itblueprints.sysagent;

import lombok.val;

import java.util.Map;

public class Arguments extends MapData {

    public void add(Arguments other){
        this.data.putAll(other.data);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    //---------------------------------------------------
    public static Arguments from(Map<String, Object> map){
        val args = new Arguments();
        args.loadFrom(map);
        return args;
    }

}
