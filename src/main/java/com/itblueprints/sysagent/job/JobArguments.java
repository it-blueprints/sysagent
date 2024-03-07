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

package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.internal.MapData;
import lombok.val;

import java.util.Map;

public class JobArguments extends MapData {

    @Override
    public String toString() {
        return super.toString();
    }

    //---------------------------------------------------
    public static JobArguments from(Map<String, Object> map){
        val args = new JobArguments();
        args.loadFrom(map);
        return args;
    }

    //---------------------------------------------------
    public static JobArguments of(String k, Object v){
        return MapData.put(k, v, new JobArguments());
    }
    public static JobArguments of(String k1, Object v1, String k2, Object v2){
        return MapData.put(k2, v2, of(k1, v1));
    }
    public static JobArguments of(String k1, Object v1, String k2, Object v2,
                                  String k3, Object v3){
        return MapData.put(k3, v3, of(k1, v1, k2, v2));
    }
    public static JobArguments of(String k1, Object v1, String k2, Object v2,
                                  String k3, Object v3, String k4, Object v4){
        return MapData.put(k4, v4, of(k1, v1, k2, v2, k3, v3));
    }
    public static JobArguments of(String k1, Object v1, String k2, Object v2,
                                  String k3, Object v3, String k4, Object v4,
                                  String k5, Object v5){
        return MapData.put(k5, v5, of(k1, v1, k2, v2, k3, v3, k4, v4));
    }
    public static JobArguments of(String k1, Object v1, String k2, Object v2,
                                  String k3, Object v3, String k4, Object v4,
                                  String k5, Object v5, String k6, Object v6){
        return MapData.put(k6, v6, of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5));
    }

}
