package com.itblueprints.sysagent;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public abstract class MapData {

    protected ConcurrentHashMap<String, Object> data = new ConcurrentHashMap<>();

    public int asInt(String key){
        return (Integer) get(key);
    }

    public boolean asBool(String key){
        return (Boolean) get(key);
    }

    public String asString(String key){
        return (String) get(key);
    }

    public long asLong(String key){
        return (Long) get(key);
    }

    public LocalDateTime asTime(String key){
        return (LocalDateTime) get(key);
    }

    public LocalDate asDate(String key){
        return (LocalDate) get(key);
    }

    public <T> T asType(String key, Class<T> clazz){
        return (T) get(key);
    }

    private Object get(String key) {
        if(!data.containsKey(key)) throw new RuntimeException("Key "+key+" not present");
        return data.get(key);
    }

    public void put(String key, Object value) {
        data.put(key, value);
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
