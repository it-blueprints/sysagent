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

package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.internal.MapData;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

import java.util.Map;

@Getter
@Setter
public class Partition extends MapData {

    private Integer partitionNum;

    private Integer totalPartitions;

    @Override
    public String toString() {
        return super.toString();
    }

    //---------------------------------------------------
    public static Partition from(Map<String, Object> map){
        val args = new Partition();
        args.loadFrom(map);
        return args;
    }
}
