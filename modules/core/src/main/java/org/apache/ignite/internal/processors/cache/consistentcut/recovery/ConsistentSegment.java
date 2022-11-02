/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.consistentcut.recovery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Represents consistent continuos segment on single node on specified topology version. It's safe to recover on every
 * Consistent Cut within this segment, but it might contain some inconsistent cuts {@link #inconsistent}.
 */
public class ConsistentSegment implements Comparable<ConsistentSegment> {
    /** First available Cut of this segment. */
    @GridToStringInclude
    private long first = -1;

    /** Last available Cut of this segment. */
    @GridToStringInclude
    private long last = -1;

    /** */
    @GridToStringInclude
    private AffinityTopologyVersion topVer;

    /** Collection of inconsistent Cuts versions within this segment. */
    @GridToStringInclude
    private Set<Long> inconsistent;

    /** Whether the node was already recovered before on the {@link #last} version. */
    @GridToStringInclude
    private boolean recovered;

    /** */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** */
    boolean addVersion(ConsistentCutVersion ver) {
        if (first == -1) {
            first = last = ver.version();
            topVer = ver.topologyVersion();

            return true;
        }

        if (topVer.equals(ver.topologyVersion())) {
            last = ver.version();

            return true;
        }

        return false;
    }

    /** */
    void markInconsistent(ConsistentCutVersion ver) {
        if (inconsistent == null)
            inconsistent = new HashSet<>();

        inconsistent.add(ver.version());
    }

    /** */
    void setRecoveredVersion(ConsistentCutVersion ver) {
        assert !inconsistent.contains(ver.version()) : ver;

        recovered = true;

        last = ver.version();
    }

    /** */
    boolean containsVersion(ConsistentCutVersion ver) {
        return topVer.equals(ver.topologyVersion()) && first <= ver.version() && last >= ver.version();
    }

    /** */
    ConsistentCutVersion last() {
        return new ConsistentCutVersion(last, topVer);
    }

    /** */
    boolean recovered() {
        return recovered;
    }

    /** */
    void recovered(ConsistentCutVersion ver) {
        recovered = true;
        last = ver.version();
    }

    /** */
    public List<ConsistentCutVersion> segment() {
        List<ConsistentCutVersion> segment = new ArrayList<>((int)(last - first + 1));

        for (long ver = last; ver >= first; ver--) {
            if (inconsistent != null && inconsistent.contains(ver))
                continue;

            segment.add(new ConsistentCutVersion(ver, topVer));
        }

        return segment;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConsistentSegment.class, this);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull ConsistentSegment o) {
        if (topVer.equals(o.topVer))
            return 0;

        int verCmp = Long.compare(last, o.last);

        return verCmp != 0 ? verCmp : topVer.compareTo(o.topVer);
    }
}
