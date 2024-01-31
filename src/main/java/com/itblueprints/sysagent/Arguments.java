package com.itblueprints.sysagent;

import lombok.val;

public class Arguments extends MapData {

    public void add(Arguments other){
        this.data.putAll(other.data);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    //---------------------------------------------------
    public static Arguments of(String key, Object value){
        val args = new Arguments();
        args.put(key, value);
        return args;
    }
    public static Arguments of(String key1, Object value1,
                               String key2, Object value2){
        val args = Arguments.of(key1, value1);
        args.put(key2, value2);
        return args;
    }
    public static Arguments of(String key1, Object value1,
                               String key2, Object value2,
                               String key3, Object value3){
        val args = Arguments.of(key1, value1, key2, value2);
        args.put(key3, value3);
        return args;
    }
    public static Arguments of(String key1, Object value1,
                               String key2, Object value2,
                               String key3, Object value3,
                               String key4, Object value4){
        val args = Arguments.of(key1, value1, key2, value2, key3, value3);
        args.put(key4, value4);
        return args;
    }

}
