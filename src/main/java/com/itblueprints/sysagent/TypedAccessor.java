package com.itblueprints.sysagent;

import lombok.val;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

public interface TypedAccessor {

    Object get(String key);

    default int getAsInt(String key){
        return (Integer) get(key);
    }

    default boolean getAsBool(String key){
        return (Boolean) get(key);
    }

    default String getAsString(String key){
        return (String) get(key);
    }

    default long getAsLong(String key){
        return (Long) get(key);
    }

    default LocalDateTime getAsTime(String key){
        val time = get(key);
        if(time instanceof Date) return Utils.toDateTime((Date)time);
        else return (LocalDateTime) get(key);
    }

    default LocalDate getAsDate(String key){
        val dt = get(key);
        if(dt instanceof Date) return Utils.toDate((Date)dt);
        else return (LocalDate) get(key);
    }

}
