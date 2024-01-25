package com.itblueprints.sysagent;

import lombok.val;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

public interface TypedAccessor {

    Object get(String key);

    default int asInt(String key){
        return (Integer) get(key);
    }

    default boolean asBool(String key){
        return (Boolean) get(key);
    }

    default String asString(String key){
        return (String) get(key);
    }

    default long asLong(String key){
        return (Long) get(key);
    }

    default LocalDateTime asTime(String key){
        val time = get(key);
        if(time instanceof Date) return Utils.toDateTime((Date)time);
        else return (LocalDateTime) get(key);
    }

    default LocalDate asDate(String key){
        val dt = get(key);
        if(dt instanceof Date) return Utils.toDate((Date)dt);
        else return (LocalDate) get(key);
    }

    default <T> T asType(String key, Class<T> clazz){
        return (T) get(key);
    }

}
