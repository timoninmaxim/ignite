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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutVersion;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CLUSTER_SNAPSHOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_RECOVERY_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_START_RECORD;
import static org.apache.ignite.internal.processors.cache.consistentcut.recovery.ConsistentCutRecovery.walIter;

/**
 * Collects finished consistent cuts in WAL archive directory, and returns them in reverse order.
 * It also checks that WAL archive contains record of specified ClusterSnapshot, and fails if it wasn't found.
 */
public class NodeConsistentCutSegments {
    /** */
    private final IgniteLogger log;

    /** Path to WAL archive. */
    private final File[] walArchiveFiles;

    /** Cluster snapshot name. */
    private final String snpName;

    /** */
    public NodeConsistentCutSegments(IgniteLogger log, File[] walArchiveFiles, String snpName) {
        this.log = log;
        this.walArchiveFiles = walArchiveFiles;
        this.snpName = snpName;
    }

    /** */
    public List<ConsistentSegment> get() throws IgniteCheckedException {
        WALIterator walIt = walIter(walArchiveFiles, log, (rec, ptr) ->
            rec == CLUSTER_SNAPSHOT
                || rec == CONSISTENT_CUT_FINISH_RECORD
                || rec == CONSISTENT_CUT_START_RECORD
                || rec == CONSISTENT_CUT_RECOVERY_RECORD
        );

        WALPointer snpPtr = findClusterSnapshotRecord(walIt);

        if (snpPtr == null) {
            List<String> wals = Arrays.stream(walArchiveFiles).map(File::getName).collect(Collectors.toList());

            String err = String.format("Fail to find ClusterSnapshot record for snapshot %s in WAL files: %s", snpName, wals);

            U.error(log, err);

            throw new IgniteCheckedException(err);
        }

        ConsistentCutStartRecord currStartRec = null;

        ConsistentSegments segments = new ConsistentSegments();

        while (walIt.hasNext()) {
            WALRecord rec = walIt.next().getValue();

            if (rec.type() == CONSISTENT_CUT_START_RECORD) {
                if (currStartRec != null)
                    segments.markInconsistent(currStartRec.version());

                currStartRec = (ConsistentCutStartRecord)rec;

                segments.addVersion(currStartRec.version());
            }
            else if (rec.type() == CONSISTENT_CUT_FINISH_RECORD) {
                if (currStartRec != null)
                    currStartRec = null;
            }
            else if (rec.type() == CONSISTENT_CUT_RECOVERY_RECORD)
                segments.markRecovery(((ConsistentCutRecoveryRecord)rec).version());
        }

        List<ConsistentSegment> segs = segments.segments();

        log.info(String.format("Found %d available Consistent segments: " + segs, segs.size()));

        // Read the latest cuts first.
        Collections.reverse(segs);

        return segs;
    }

    /**
     * Finds point of cluster snapshot creation.
     *
     * @return Pointer to {@link ClusterSnapshotRecord} related to specifued snapshot name, or {@code null} if not found.
     */
    private @Nullable WALPointer findClusterSnapshotRecord(WALIterator it) {
        while (it.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> next = it.next();

            if (next.getValue().type() == CLUSTER_SNAPSHOT)
                if (((ClusterSnapshotRecord)next.getValue()).clusterSnapshotName().equals(snpName))
                    return next.getKey();
        }

        return null;
    }

    /** */
    private static class ConsistentSegments {
        /** */
        List<ConsistentSegment> segments = new ArrayList<>();

        /** */
        ConsistentSegment curSeg;

        /** */
        void addVersion(ConsistentCutVersion ver) {
            if (curSeg == null)
                curSeg = new ConsistentSegment();

            if (!curSeg.addVersion(ver)) {
                segments.add(curSeg);

                curSeg = new ConsistentSegment();
                curSeg.addVersion(ver);
            }
        }

        /** */
        List<ConsistentSegment> segments() {
            segments.add(curSeg);

            curSeg = null;

            return segments;
        }

        /** */
        void markInconsistent(ConsistentCutVersion ver) {
            curSeg.markInconsistent(ver);
        }

        /** */
        void markRecovery(ConsistentCutVersion ver) {
            segments.add(curSeg);

            curSeg = null;

            Iterator<ConsistentSegment> segIt = segments.iterator();

            // Cleans all segments appeared after recovery.
            boolean clean = false;

            while(segIt.hasNext()) {
                if (clean)
                    segIt.remove();

                ConsistentSegment seg = segIt.next();

                if (seg.containsVersion(ver)) {
                    seg.recovered(ver);

                    clean = true;
                }
            }
        }
    }
}
