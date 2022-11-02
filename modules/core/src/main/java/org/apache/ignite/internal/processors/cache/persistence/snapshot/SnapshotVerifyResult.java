/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Result of verifying cluster snapshot for recovery.
 */
public class SnapshotVerifyResult {
    /** Snapshot partitions verify task result. */
    private SnapshotPartitionsVerifyTaskResult partVerifyRes;

    /** Optional, the latest Cut version that it's possible to recover cluster on. */
    private @Nullable Collection<ConsistentCutVersion> cutVers;

    /** */
    private Map<ClusterNode, Exception> exceptions;

    /** */
    SnapshotVerifyResult(SnapshotPartitionsVerifyTaskResult partVerifyRes, @Nullable Collection<ConsistentCutVersion> cutVers) {
        this.partVerifyRes = partVerifyRes;
        this.cutVers = cutVers;
    }

    /** */
    SnapshotVerifyResult(Map<ClusterNode, Exception> exceptions) {
        this.exceptions = exceptions;
    }

    /** */
    public Map<ClusterNode, Exception> exceptions() {
        return exceptions;
    }

    /** */
    public Map<ClusterNode, List<SnapshotMetadata>> metas() {
        return partVerifyRes.metas();
    }

    /** */
    public Collection<ConsistentCutVersion> cutVersions() {
        return cutVers;
    }
}

