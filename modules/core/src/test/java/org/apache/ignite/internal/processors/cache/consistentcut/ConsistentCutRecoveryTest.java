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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.WalTestUtils;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTask;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTaskArg;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyTaskResult;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER;

/** */
public class ConsistentCutRecoveryTest extends AbstractConsistentCutBlockingTest {
    /** */
    private static final int PARTS = 10;

    /** */
    private static final String CACHE2 = CACHE + "2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setIncludeEventTypes(EVT_CONSISTENCY_VIOLATION);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE), cacheConfiguration(CACHE2));

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(nodes());

        grid(0).cluster().state(ClusterState.ACTIVE);

        loadData(0, 1000);

        snp(grid(0)).createSnapshot(SNP).get();
    }

    /** */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String name) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>(name)
            .setBackups(backups())
            .setDataRegionName("consistent-cut-persist")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        // Marke ReadRepair works faster.
        ccfg.setAffinity(new RendezvousAffinityFunction().setPartitions(PARTS));

        return ccfg;
    }

    /** */
    @Test
    public void testRecoveryOnClusterSnapshotOnly() throws Exception {
        loadAndCreateSnapshots(3);

        restartWithCleanPersistence();

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 0).get(getTestTimeout());

        checkData(1_000, CACHE);
        checkData(1_000, CACHE2);
    }

    /** */
    @Test
    public void testRecoveryOnIncrementalSnapshot() throws Exception {
        loadAndCreateSnapshots(3);

        for (int i = 1; i <= 3; i++) {
            restartWithCleanPersistence();

            snp(grid(0)).restoreIncrementalSnapshot(SNP, null, i).get(getTestTimeout());

            checkData(1_000 + i * 1_000, CACHE);
            checkData(1_000 + i * 1_000, CACHE2);
        }
    }

    /** */
    @Test
    public void testRecoverySingleCacheGroup() throws Exception {
        loadAndCreateSnapshots(1);

        restartWithCleanPersistence();

        snp(grid(0)).restoreIncrementalSnapshot(SNP, F.asSet(CACHE), 1).get(getTestTimeout());

        checkData(2_000, CACHE);
        assertNull(grid(0).cache(CACHE2));
    }

    /** */
    @Test
    public void testNonExistentSnapshotFailed() throws Exception {
        loadAndCreateSnapshots(1);

        restartWithCleanPersistence();

        GridTestUtils.assertThrows(log, () ->
            snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 2).get(getTestTimeout()),
            IgniteException.class,
            null);
    }

    /** */
    @Test
    public void testRecoveryOnClusterSnapshotIfNoWalsOnSingleNode() throws Exception {
        loadAndCreateSnapshots(1);

        restartWithCleanPersistence();

        File rm = Paths.get(U.defaultWorkDirectory())
            .resolve(DFLT_SNAPSHOT_DIRECTORY)
            .resolve(SNP)
            .resolve(IgniteSnapshotManager.INC_SNP_DIR)
            .resolve(U.maskForFileName(consistentIds.get(1).toString()))
            .resolve("0000000000000001")
            .resolve("0000000000000000.wal.zip")
            .toFile();

        assertTrue(U.delete(rm));

        GridTestUtils.assertThrowsAnyCause(log,
            () -> grid(0).context().cache().context().snapshotMgr()
                .restoreSnapshot(SNP, null, null, 1)
                .get(),
            IgniteCheckedException.class, "WAL segments weren't found.");

        assertNull(grid(0).cache(CACHE));
        assertNull(grid(0).cache(CACHE2));
    }

    /** */
    @Test
    public void testFailedOnCorruptedWalSegment() throws Exception {
        loadAndCreateSnapshots(1);

        restartWithCleanPersistence();

        corruptIncrementalSnapshot(1, 1);

        GridTestUtils.assertThrows(log,
            () -> grid(0).context().cache().context().snapshotMgr()
                .restoreSnapshot(SNP, null, null, 1)
                .get(),
            IgniteException.class, null
        );

        assertNull(grid(0).cache(CACHE));
        assertNull(grid(0).cache(CACHE2));
    }

    /** */
    @Test
    public void testRestartAfterRecovery() throws Exception {
        loadAndCreateSnapshots(1);

        restartWithCleanPersistence();

        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot(SNP, null, null, 1)
            .get();

        stopAllGrids();

        startGrids(nodes());

        checkData(2_000, CACHE);
        checkData(2_000, CACHE2);
    }

    /** */
    private void loadAndCreateSnapshots(int incSnpCnt) {
        int from = 1000;

        for (int i = 0; i < incSnpCnt; i++) {
            loadData(from, from + 1000);

            snp(grid(0)).createIncrementalSnapshot(SNP).get();

            from += 1000;
        }
    }

    /** */
    private void checkData(int expSize, String cache) {
        int size = grid(0).cache(cache).query(new ScanQuery<Integer, Integer>()).getAll().size();

        assertEquals(expSize, size);

        // Idle verify - OK.
        for (int i = 0; i < nodes(); i++)
            idleVerify(grid(i));

        // Read repair check - OK.
        AtomicBoolean readRepairCheckFailed = new AtomicBoolean(false);

        grid(0).events().remoteListen(null,
            (IgnitePredicate<Event>)e -> {
                assert e instanceof CacheConsistencyViolationEvent;

                readRepairCheckFailed.set(true);

                return true;
            },
            EVT_CONSISTENCY_VIOLATION);

        Set<Integer> parts = IntStream.range(0, PARTS).boxed().collect(Collectors.toSet());

        VisorConsistencyTaskResult res = grid(0).compute().execute(
            VisorConsistencyRepairTask.class,
            new VisorTaskArgument<>(
                G.allGrids().stream().map(ign -> ign.cluster().localNode().id()).collect(Collectors.toList()),
                new VisorConsistencyRepairTaskArg(cache, parts, ReadRepairStrategy.CHECK_ONLY),
                false));

        assertFalse(res.message(), res.cancelled());
        assertFalse(res.message(), res.failed());

        assertFalse(readRepairCheckFailed.get());
    }

    /** Prepare for snapshot restoring - restart grids, with clean persistence. */
    private void restartWithCleanPersistence() throws Exception {
        stopAllGrids();

        assertTrue(U.delete(Paths.get(U.defaultWorkDirectory(), "cp").toFile()));

        deleteNodesDirs(DFLT_STORE_DIR, DFLT_BINARY_METADATA_PATH, DFLT_WAL_PATH, DFLT_WAL_ARCHIVE_PATH);

        startGrids(nodes());

        grid(0).cluster().state(ClusterState.ACTIVE);

        // Caches are configured with IgniteConiguration, need to destroy them before restoring snapshot.
        grid(0).destroyCaches(F.asList(CACHE, CACHE2));

        for (int i = 0; i < nodes(); i++) {
            String nodeFolder = U.maskForFileName(consistentIds.get(i).toString());

            Path path = Paths.get(U.defaultWorkDirectory(), DFLT_STORE_DIR, nodeFolder);

            GridTestUtils.waitForCondition(() -> !path.toFile().exists(), 1_000, 10);
        }
    }

    /** */
    private void deleteNodesDirs(String... dirs) throws IgniteCheckedException {
        for (int i = 0; i < nodes(); i++) {
            String nodeFolder = U.maskForFileName(consistentIds.get(i).toString());

            for (String dir: dirs) {
                File p = Paths.get(U.defaultWorkDirectory(), dir, nodeFolder).toFile();

                assertTrue(U.delete(p));
            }
        }
    }

    /** */
    private void loadData(int from, int to) {
        Random rnd = new Random();

        for (int i = from; i < to; i++) {
            try (Transaction tx = grid(0).transactions().txStart()) {
                grid(0).cache(CACHE).put(i, rnd.nextInt());
                grid(0).cache(CACHE2).put(i, rnd.nextInt());

                tx.commit();
            }
        }
    }

    /** Corrupts WAL segment in incremental snapshot. */
    private void corruptIncrementalSnapshot(int nodeIdx, int incIdx) throws Exception {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        File incSnpDir = grid(nodeIdx).context().cache().context().snapshotMgr()
            .incrementalSnapshotLocalDir(SNP, null, incIdx);

        File[] incSegs = incSnpDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(incSegs[0]);

        WALIterator it = factory.iterator(params);

        for (int i = 0; i < 400; i++)
            it.next();

        WALPointer corruptPtr = it.next().getKey();

        WalTestUtils.corruptWalSegmentFile(new FileDescriptor(incSegs[0]), corruptPtr);
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 2;
    }
}
