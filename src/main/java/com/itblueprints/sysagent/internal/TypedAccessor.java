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

import lombok.val;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

public interface TypedAccessor {

    Object get(String key);

    default int getInt(String key){
        return (Integer) get(key);
    }

    default boolean getBool(String key){
        return (Boolean) get(key);
    }

    default String getString(String key){
        return (String) get(key);
    }

    default long getLong(String key){
        return (Long) get(key);
    }

    default LocalDateTime getTime(String key){
        val time = get(key);
        if(time instanceof Date) return Utils.toDateTime((Date)time);
        else return (LocalDateTime) get(key);
    }

    default LocalDate getDate(String key){
        val dt = get(key);
        if(dt instanceof Date) return Utils.toDate((Date)dt);
        else return (LocalDate) get(key);
    }

}
