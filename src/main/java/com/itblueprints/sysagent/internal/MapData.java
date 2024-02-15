/*
 * Copyright 2023-2024 the original author or authors.
 *
 * Licensed under the Apache Software License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.itblueprints.sysagent.internal;

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
