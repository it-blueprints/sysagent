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

package com.itblueprints.sysagent.internal.cluster;

import lombok.ToString;

import java.time.LocalDateTime;
import java.util.List;

/**
 * A transient data structure that holds the current state of this node
 * as welll as the cluster it is running in
 */
@ToString
public class NodeInfo {
    /**
     * The time now
     */
    public LocalDateTime timeNow;

    /**
     * The id of this node. This is the id of the corresponding NodeRecord in the DB
     */
    public String nodeId;

    /**
     * If this node is the manager node
     */
    public boolean isManager;

    /**
     * If this node is busy i.e. there is a Step that is
     * currently being processed
     */
    public boolean isNodeBusy;

    /**
     * The list of node ids for nodes that are dead i.e.
     * faield to update the life lease
     */
    public List<String> deadNodeIds;
}
