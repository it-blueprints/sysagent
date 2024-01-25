package com.itblueprints.sysagent;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.util.concurrent.ConcurrentHashMap;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public abstract class MapData implements TypedAccessor {

    protected ConcurrentHashMap<String, Object> data = new ConcurrentHashMap<>();

    public Object get(String key) {
        if(!data.containsKey(key)) throw new RuntimeException("Key "+key+" not present");
        return data.get(key);
    }

    public void put(String key, Object value) {
        data.put(key, value);
    }

    public boolean contains(String key) { return data.contains(key); }

    @Override
    public String toString() {
        return data.toString();
    }
}
