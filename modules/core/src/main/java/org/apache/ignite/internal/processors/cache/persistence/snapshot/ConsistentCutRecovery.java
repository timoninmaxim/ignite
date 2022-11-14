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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheStripedExecutor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CLUSTER_SNAPSHOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_START_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER;

/**
 * Class is responsible for recovering data of incremental snapshots.
 */
public class ConsistentCutRecovery {
    /** Cluster snapshot name. */
    @GridToStringInclude
    private final String snpName;

    /** Cluster snapshot path. */
    @GridToStringInclude
    private final String snpPath;

    /** Incremental snapshot index. */
    @GridToStringInclude
    private final long incIdx;

    /** Striped executor, groups operations by cache group and partition. */
    private final CacheStripedExecutor exec;

    /** */
    private final GridCacheProcessor cacheProc;

    /** */
    private final IgniteLogger log;

    /** */
    private final Set<Integer> cacheIds;

    /** */
    public ConsistentCutRecovery(
        GridKernalContext kernalCtx,
        String snpName,
        @Nullable String snpPath,
        long incIdx,
        Set<Integer> cacheIds
    ) {
        this.snpName = snpName;
        this.snpPath = snpPath;
        this.cacheIds = cacheIds;
        this.incIdx = incIdx;

        cacheProc = kernalCtx.cache();

        exec = new CacheStripedExecutor(kernalCtx.pools().getStripedExecutorService());
        log = kernalCtx.log(ConsistentCutRecovery.class);
    }

    /**
     * Start recovery of incremental snapshot.
     *
     * @return Future that completes after all incremental snapshot data is restored.
     */
    public IgniteInternalFuture<Boolean> start() {
        GridFutureAdapter<Boolean> res = new GridFutureAdapter<>();

        cacheProc.context().kernalContext().pools().getSnapshotExecutorService().submit(() -> {
            try {
                walEnabled(false);

                recover();

                res.onDone(true);
            }
            catch (Throwable e) {
                res.onDone(e);
            }
            finally {
                walEnabled(true);
            }
        });

        return res;
    }

    /** */
    private ConsistentCutFinishRecord readFinishRecord(File segment) throws IgniteCheckedException, IOException {
        IncrementalSnapshotMetadata incSnpMeta = cacheProc.context().snapshotMgr()
            .readIncrementalSnapshotMetadata(snpName, snpPath, incIdx);

        WALIterator it = walIter(log, segment);

        while (it.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> item = it.next();

            if (item.getValue() instanceof ConsistentCutFinishRecord) {
                ConsistentCutFinishRecord finRec = (ConsistentCutFinishRecord)item.getValue();

                if (!finRec.cutId().equals(incSnpMeta.requestId()))
                    throw new IgniteCheckedException("Increment snapshot ID doesn't match ConsistentCutFinishRecord");

                return finRec;
            }
        }

        throw new IgniteCheckedException("ConsistentCutFinishRecord wasn't found for snapshot " + incSnpMeta);
    }

    /** */
    private void recover() throws IgniteCheckedException, IOException {
        File[] segments = walSegments();

        if (F.isEmpty(segments))
            throw new IgniteCheckedException("WAL segments weren't found.");

        ConsistentCutFinishRecord finRec = readFinishRecord(segments[segments.length - 1]);

        IgnitePredicate<GridCacheVersion> txVerFilter = txVer -> !finRec.after().contains(txVer);

        WALIterator it = walIter(log, segments);

        boolean start = false;

        while (it.hasNext()) {
            WALRecord rec = it.next().getValue();

            if (rec.type() == CLUSTER_SNAPSHOT) {
                start = true;

                continue;
            }
            else if (!start)
                continue;

            if (rec.type() == CONSISTENT_CUT_START_RECORD) {
                ConsistentCutStartRecord startRec = (ConsistentCutStartRecord)rec;

                if (!startRec.cutId().equals(finRec.cutId()))
                    continue;

                txVerFilter = v -> finRec.before().contains(v);
            }
            else if (rec.type() == DATA_RECORD_V2) {
                DataRecord data = (DataRecord)rec;

                DataEntry entry = data.writeEntries().get(0);

                if (txVerFilter.apply(entry.nearXidVersion()))
                    applyDataRecord(data);
            }
        }

        exec.awaitApplyComplete();
    }

    /** */
    private File[] walSegments() {
        File[] segments = new File[0];

        for (int i = 1; i <= incIdx; i++) {
            File incSnpDir = cacheProc.context().cache().context().snapshotMgr()
                .incrementalSnapshotLocalDir(snpName, snpPath, i);

            File[] incSegs = incSnpDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

            if (incSegs == null)
                return segments;

            int segLen = segments.length;

            segments = Arrays.copyOf(segments, segLen + incSegs.length);

            System.arraycopy(incSegs, 0, segments, segLen, incSegs.length);
        }

        return segments;
    }

    /** */
    private void walEnabled(boolean enabled) {
        for (int cacheId: cacheIds) {
            int grpId = cacheProc.cacheDescriptor(cacheId).groupId();

            CacheGroupContext grp = cacheProc.cacheGroup(grpId);

            grp.localWalEnabled(enabled, true);
        }
    }

    /**  */
    private boolean walRecordFilter(DataRecord dataRec, IgnitePredicate<GridCacheVersion> txVerFilter) {
        DataEntry entry = dataRec.writeEntries().get(0);

        return txVerFilter.apply(entry.nearXidVersion());
    }

    /** Concurrently applies DataRecord to Ignite. */
    private void applyDataRecord(DataRecord dataRec) {
        int entryCnt = dataRec.entryCount();

        for (int i = 0; i < entryCnt; i++) {
            DataEntry dataEntry = dataRec.get(i);

            int cacheId = dataEntry.cacheId();

            DynamicCacheDescriptor desc = cacheProc.cacheDescriptor(cacheId);

            if (desc == null || !cacheIds.contains(desc.groupId()))
                continue;

            exec.submit(() -> {
                GridCacheContext<?, ?> cacheCtx = cacheProc.context().cacheContext(cacheId);

                try {
                    applyDataEntry(cacheCtx, dataEntry);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to apply data entry, dataEntry=" + dataEntry +
                        ", ptr=" + dataRec.position());

                    exec.onError(e);
                }
            }, desc.groupId(), dataEntry.partitionId());
        }
    }

    /**
     * @param cacheCtx Cache context to apply an update.
     * @param dataEntry Data entry to apply.
     * @throws IgniteCheckedException If failed to apply entry.
     */
    private void applyDataEntry(GridCacheContext<?, ?> cacheCtx, DataEntry dataEntry) throws IgniteCheckedException {
        int partId = dataEntry.partitionId();

        if (partId == -1)
            partId = cacheCtx.affinity().partition(dataEntry.key());

        GridDhtLocalPartition locPart = cacheCtx.topology().forceCreatePartition(partId);

        switch (dataEntry.op()) {
            case CREATE:
            case UPDATE:
                cacheCtx.offheap().update(
                    cacheCtx,
                    dataEntry.key(),
                    dataEntry.value(),
                    dataEntry.writeVersion(),
                    dataEntry.expireTime(),
                    locPart,
                    null);

                if (dataEntry.partitionCounter() != 0)
                    cacheCtx.offheap().dataStore(locPart).updateInitialCounter(dataEntry.partitionCounter() - 1, 1);

                break;

            case DELETE:
                cacheCtx.offheap().remove(cacheCtx, dataEntry.key(), partId, locPart);

                if (dataEntry.partitionCounter() != 0)
                    cacheCtx.offheap().dataStore(locPart).updateInitialCounter(dataEntry.partitionCounter() - 1, 1);

                break;

            default:
                // do nothing
        }
    }

    /**
     * @param log Ignite logger.
     * @param segments WAL segments.
     *
     * @return Iterator over WAL archive.
     */
    public static WALIterator walIter(IgniteLogger log, File... segments) throws IgniteCheckedException {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(segments);

        return factory.iterator(params);
    }
}
