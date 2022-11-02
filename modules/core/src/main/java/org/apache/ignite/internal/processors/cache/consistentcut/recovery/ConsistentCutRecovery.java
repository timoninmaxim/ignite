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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCut;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheStripedExecutor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CLUSTER_SNAPSHOT;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_RECOVERY_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_START_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;

/**
 * Recovers Ignite to a specific point in time. This point is {@link ConsistentCut}. It's safe to recovery until this point
 * on every node independently. Algorithm of recovery is:
 * 1. Reach {@link ClusterSnapshotRecord} of snapshot used for recovery in WAL.
 * 2. Apply WAL segments discretely: after it applied previous batch of updates until {@link ConsistentCutFinishRecord},
 *    it reaches next {@link ConsistentCutStartRecord} storing data entries in buffer.
 * 3. Check {@link ConsistentCutVersion} of the record.
 * 4. If the version less than recoverCutVer it applies all data entries until the related {@link ConsistentCutFinishRecord}.
 * 5. If the version equals to recoverCutVer it firstly reaches the related {@link ConsistentCutFinishRecord}:
 *    a) It applies data entries until this {@link ConsistentCutStartRecord} excluding {@link ConsistentCutFinishRecord#after()}.
 *    b) It applies {@link ConsistentCutFinishRecord#before()} entries to this {@link ConsistentCutFinishRecord}.
 */
public class ConsistentCutRecovery {
    /** Cluster snapshot name. */
    @GridToStringInclude
    private final String snpName;

    /** Path to WAL archive files for PITR. */
    @GridToStringInclude
    private final File[] walArchiveFiles;

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
        Set<Integer> cacheIds
    ) {
        this.snpName = snpName;
        this.cacheIds = cacheIds;

        cacheProc = kernalCtx.cache();

        exec = new CacheStripedExecutor(kernalCtx.pools().getStripedExecutorService());
        log = kernalCtx.log(ConsistentCutRecovery.class);

        walArchiveFiles = Arrays.stream(cacheProc.context().wal().walArchiveFiles())
            .map(FileDescriptor::file)
            .toArray(File[]::new);
    }

    /** */
    private void prepare() {
        // It's required to set new segment idx for WAL after recovering. Then stop WAL.
        cacheProc.context().wal().onDeActivate(cacheProc.context().kernalContext());

        // Skip allocating memory for records that will be ignored.
        walEnabled(false);
    }

    /** */
    private void finish(@Nullable ConsistentCutVersion cutVer, @Nullable WALPointer ptr) throws IgniteCheckedException {
        // Just rollback WAL operations.
        if (ptr == null || cutVer == null) {
            cacheProc.context().wal().resumeLogging(null, false);

            walEnabled(true);

            return;
        }

        cacheProc.context().consistentCutMgr().cutVersion(cutVer);
        cacheProc.context().wal().resumeLogging(ptr, true);

        walEnabled(true);

        cacheProc.context().wal().log(new ConsistentCutRecoveryRecord(cutVer));
    }

    /**
     * Performs Ignite node recovery.
     *
     * @param recoverCutVers Consistent Cut version to recover on.
     */
    public IgniteInternalFuture<Boolean> start(Collection<ConsistentCutVersion> recoverCutVers) {
        GridFutureAdapter<Boolean> res = new GridFutureAdapter<>();

        cacheProc.context().kernalContext().pools().getSnapshotExecutorService().submit(() -> {
            ConsistentCutVersion cutVer = null;

            try {
                prepare();

                Set<ConsistentCutVersion> prevRecoverVers = recoverCutVers.size() == 1 ? null : new HashSet<>();

                Iterator<ConsistentCutVersion> it = recoverCutVers.iterator();
                cutVer = it.next();

                while (it.hasNext()) {
                    prevRecoverVers.add(cutVer);

                    cutVer = it.next();
                }

                WALPointer ptr = recover(prevRecoverVers, cutVer);

                finish(cutVer, ptr);

                res.onDone(true);
            }
            catch (Throwable e) {
                try {
                    finish(cutVer, null);

                    res.onDone(e);

                } catch (IgniteCheckedException err) {
                    U.error(log, "Failed to resume WAL logging.", e);

                    throw new IgniteException(err);
                }
            }
        });

        return res;
    }

    /** */
    private WALPointer recover(
        @Nullable Set<ConsistentCutVersion> prevRecoverCutVers,
        ConsistentCutVersion recoverCutVer
    ) throws IgniteCheckedException {
        WALIterator it = walIter(walArchiveFiles, log, (recType, ptr) ->
            recType == CLUSTER_SNAPSHOT ||
                recType == CONSISTENT_CUT_START_RECORD ||
                recType == CONSISTENT_CUT_FINISH_RECORD ||
                recType == CONSISTENT_CUT_RECOVERY_RECORD ||
                recType == DATA_RECORD_V2
        );

        WALPointer sntPtr = reachClusterSnapshot(it);

        if (sntPtr == null)
            throw new IgniteCheckedException("ClusterSnapshotRecord for snapshot '" + snpName + "' not found.");

        WALPointer lastRecPtr = recover(it, prevRecoverCutVers, recoverCutVer);

        exec.awaitApplyComplete();

        return lastRecPtr;
    }

    /** */
    private void walEnabled(boolean enabled) {
        for (int cacheId: cacheIds) {
            int grpId = cacheProc.cacheDescriptor(cacheId).groupId();

            CacheGroupContext grp = cacheProc.cacheGroup(grpId);

            grp.localWalEnabled(enabled, true);
        }
    }

    /** */
    private WALPointer recover(
        WALIterator waltIt,
        @Nullable Set<ConsistentCutVersion> prevRecoverCutVers,
        ConsistentCutVersion recoverCutVer
    ) throws IgniteCheckedException {
        log.info("Recovery process: " + prevRecoverCutVers + " " + recoverCutVer);

        BufferWalIterator bufWalIt = new BufferWalIterator(waltIt);

        ConsistentCutVersion currCutVer;

        // Discrete parsing WAL.
        while (bufWalIt.hasNext()) {
            bufWalIt.mode(BufferWalIterator.BufferedMode.STORE);

            WALRecord rec = bufWalIt.next().getValue();

            if (rec.type() == CONSISTENT_CUT_START_RECORD) {
                currCutVer = ((ConsistentCutStartRecord)rec).version();

                boolean prevRecVer = prevRecoverCutVers != null && prevRecoverCutVers.contains(currCutVer);

                if (!prevRecVer && currCutVer.compareTo(recoverCutVer) < 0) {
                    bufWalIt.resetBuffer();
                    bufWalIt.mode(BufferWalIterator.BufferedMode.CLEAN);

                    applyWal(bufWalIt, CONSISTENT_CUT_FINISH_RECORD);
                }
                else {
                    ConsistentCutFinishRecord finishRec = reachCutFinishRecord(bufWalIt);

                    if (finishRec == null)
                        throw new IgniteCheckedException("Failed find FinishRecord in WAL for CutVersion: " + currCutVer);

                    bufWalIt.resetBuffer();
                    bufWalIt.mode(BufferWalIterator.BufferedMode.CLEAN);

                    bufWalIt.txVerFilter(v -> !finishRec.after().contains(v));
                    applyWal(bufWalIt, CONSISTENT_CUT_START_RECORD);

                    bufWalIt.txVerFilter(v -> finishRec.before().contains(v));
                    applyWal(bufWalIt, CONSISTENT_CUT_FINISH_RECORD);

                    bufWalIt.txVerFilter(null);

                    if (prevRecVer) {
                        ConsistentCutRecoveryRecord recRec = reachCutRecoveryRecord(bufWalIt, currCutVer);

                        if (recRec == null)
                            throw new IgniteCheckedException("Failed find RecoveryRecord in WAL for CutVersion: " + currCutVer);

                        continue;
                    }

                    return bufWalIt.lastRead().orElseThrow(() ->
                        new IgniteCheckedException("Failed to extract last recovery WAL pointer."));
                }

                bufWalIt.resetBuffer();
            }
        }

        throw new IgniteCheckedException("Failed to find in WAL recovery Cut Version: " + recoverCutVer);
    }

    /**
     * Applies WAL records to IgniteCluster while not reach {@code stopRecord}.
     *
     * @param bufWalIt Iterator over WAL archive.
     * @param stopRecord After reached this record it stops parsing WAL.
     */
    private void applyWal(BufferWalIterator bufWalIt, WALRecord.RecordType stopRecord) throws IgniteCheckedException {
        while (bufWalIt.hasNext()) {
            WALRecord rec = bufWalIt.next().getValue();

            if (rec.type() == DATA_RECORD_V2)
                applyDataRecord((DataRecord)rec);
            else if (rec.type() == stopRecord)
                return;
        }

        throw new IgniteCheckedException();
    }

    /** */
    private ConsistentCutFinishRecord reachCutFinishRecord(BufferWalIterator bufWalIt) {
        while (bufWalIt.hasNext()) {
            WALRecord rec = bufWalIt.next().getValue();

            if (rec.type() == CONSISTENT_CUT_FINISH_RECORD)
                return (ConsistentCutFinishRecord)rec;
        }

        return null;
    }

    /** */
    private ConsistentCutRecoveryRecord reachCutRecoveryRecord(BufferWalIterator bufWalIt, ConsistentCutVersion cutVer) {
        while (bufWalIt.hasNext()) {
            WALRecord rec = bufWalIt.next().getValue();

            if (rec.type() == CONSISTENT_CUT_RECOVERY_RECORD) {
                ConsistentCutRecoveryRecord recRec = (ConsistentCutRecoveryRecord)rec;

                return recRec.version().equals(cutVer) ? recRec : null;
            }
        }

        return null;
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

        GridDhtLocalPartition locPart = cacheCtx.isLocal() ? null : cacheCtx.topology().forceCreatePartition(partId);

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
     * @return Pointer to {@link ClusterSnapshotRecord} to start recovery from.
     */
    private WALPointer reachClusterSnapshot(WALIterator it) {
        while (it.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> next = it.next();

            if (next.getValue().type() != WALRecord.RecordType.CLUSTER_SNAPSHOT)
                continue;

            ClusterSnapshotRecord snpRec = (ClusterSnapshotRecord)next.getValue();

            if (!snpRec.clusterSnapshotName().equals(snpName))
                continue;

            return next.getKey();
        }

        return null;
    }

    /**
     * @param walArchive WAL archive files.
     * @param log Ignite logger.
     * @param filter Optional filter of WAL records.
     * @return Iterator over WAL archive.
     */
    public static WALIterator walIter(
        File[] walArchive,
        IgniteLogger log,
        @Nullable IgniteBiPredicate<WALRecord.RecordType, WALPointer> filter
    ) throws IgniteCheckedException {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(walArchive);

        if (filter != null)
            params = params.filter(filter);

        return factory.iterator(params);
    }
}
