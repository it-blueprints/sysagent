package com.itblueprints.sysagent;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import lombok.val;

import java.util.Map;
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

    public boolean contains(String key) { return data.containsKey(key); }

    public void loadFrom(MapData other){
        val conflictingKeys = data.keySet().stream().filter(k -> other.data.containsKey(k)).toList();
        if(!conflictingKeys.isEmpty()) throw new SysAgentException("Cannot load from other MapData as there are conflicting keys - "
        + conflictingKeys);
        data = new ConcurrentHashMap<>(other.data);
    }
    public void loadFrom(Map<String, Object> map){
        data = new ConcurrentHashMap<>(map);
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
