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

import lombok.Getter;
import lombok.Setter;

/**
 * NodeRecord holding details for a running node. This record is used to check
 * if the node is still alive.When a node dies the lifeLeaseTill value is not
 * extended, thus indicating that the node is dead. The manager node then comes
 * along and clears out those NodeRecords
 */
@Getter
@Setter
public class NodeRecord extends BaseNodeRecord {

    /**
     * When the node started up
     */
    private long startedAt;

    /**
     * This is the time upto which this node should be considered to be alive. If the
     * current time is greater than this then the node is considered dead. With each
     * heartbeat a node extends the value of this field.
     */
    private long lifeLeaseTill;
}
