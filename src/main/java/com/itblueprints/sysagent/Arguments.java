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
    public static Arguments of(){
        val args = new Arguments();
        return args;
    }
    public static Arguments of(String key, Object value){
        val args = Arguments.of();
        args.put(key, value);
        return args;
    }
    public static Arguments of(String key1, Object value1, String key2, Object value2){
        val args = Arguments.of(key1, value1);
        args.put(key2, value2);
        return args;
    }
    public static Arguments of(String key1, Object value1, String key2, Object value2, String key3, Object value3){
        val args = Arguments.of(key1, value1, key2, value2);
        args.put(key3, value3);
        return args;
    }
    public static Arguments of(String key1, Object value1, String key2, Object value2, String key3, Object value3,
                               String key4, Object value4){
        val args = Arguments.of(key1, value1, key2, value2, key3, value3);
        args.put(key4, value4);
        return args;
    }
    public static Arguments of(String key1, Object value1, String key2, Object value2, String key3, Object value3,
                               String key4, Object value4, String key5, Object value5){
        val args = Arguments.of(key1, value1, key2, value2, key3, value3, key4, value4);
        args.put(key5, value5);
        return args;
    }
    public static Arguments of(String key1, Object value1, String key2, Object value2, String key3, Object value3,
                               String key4, Object value4, String key5, Object value5, String key6, Object value6){
        val args = Arguments.of(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5);
        args.put(key6, value6);
        return args;
    }

}
