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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.WalTestUtils;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationRequest;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutTest.TestConsistentCutManager.cutMgr;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/** */
@RunWith(Parameterized.class)
public class PitrOnStartedFromScratchClusterTest extends AbstractConsistentCutBlockingTest {
    /** */
    private static final int PARTS = 10;

    /** */
    private static final String CACHE2 = CACHE + "2";

    /** */
    @Parameterized.Parameter
    public boolean walCompact;

    /** */
    @Parameterized.Parameter(1)
    public int walSegments;

    // TODO: test CacheWriteSynchronizationMode ASYNC modes? in ConccurentTxsTest.
    // TODO: test without restart - just restore SNP during ordinary work.

    /** */
    @Parameterized.Parameters(name = "walCompact={0}, walSegments={1}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (boolean compact: F.asList(false, true)) {
            for (int segments: F.asList(3, 10))
                params.add(new Object[] { compact, segments } );
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.getDataStorageConfiguration()
            .setMaxWalArchiveSize(-1)
            .setWalSegments(walSegments)
            .setWalCompactionEnabled(walCompact)
            .setWalSegmentSize((int)(2 * MB));  // 2Mb, enforce faster archiving segments.

        cfg.setIncludeEventTypes(EVT_CONSISTENCY_VIOLATION);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE), cacheConfiguration(CACHE2));

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Integer, Integer> cacheConfiguration(String name) {
        CacheConfiguration<Integer, Integer> ccfg = super.cacheConfiguration(name);

        ccfg.setAffinity(new RendezvousAffinityFunction().setPartitions(PARTS));

        return ccfg;
    }

    /** */
    @Test
    public void prepareForRestoreSmokeTest() throws Exception {
        loadAndAwaitWalArchived(0, 1_000, false);

        Map<Integer, WALPointer> before = restartWithCleanPersist();

        for (int i = 0; i < nodes(); i++) {
            // Instead of restoring
            walMgr(grid(i)).resumeLogging(before.get(i), true);

            assertTrue(walMgr(grid(i)).lastArchivedSegment() >= 0);
        }

        List<Path> paths = Files.list(Paths.get(U.defaultWorkDirectory(), DFLT_STORE_DIR)).collect(Collectors.toList());

        assertEquals(nodes() + 3, paths.size());

        grid(0).createCaches(F.asList(
            cacheConfiguration(CACHE), cacheConfiguration(CACHE2)
        ));

        loadAndAwaitWalArchived(0, 1_000, false);

        checkData(1_000);
    }

    /** */
    @Test
    public void testRecoveryOnClusterSnapshotOnly() throws Exception {
        loadData(0, 1_000);

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadAndAwaitWalArchived(1_000, 2_000, true);

        restartWithCleanPersist();

        assertEquals(0, cutMgr(grid(0)).cutVersion().version());

        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot("SNP0", null, null, false)
            .get();

        checkData(1_000);
    }

    /** */
    @Test
    public void testRecoveryOnClusterSnapshot() throws Exception {
        loadData(0, 1_000);

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadAndAwaitWalArchived(1_000, 2_000, true);

        restartWithCleanPersist();

        assertEquals(0, cutMgr(grid(0)).cutVersion().version());

        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot("SNP0", null, null, true)
            .get();

        checkData(2_000);
    }

    /** */
    @Test
    public void testRecoverySingleCacheGroup() throws Exception {
        loadData(0, 1_000);

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadAndAwaitWalArchived(1_000, 2_000, true);

        restartWithCleanPersist();

        assertEquals(0, cutMgr(grid(0)).cutVersion().version());

        // TODO: check cacheId and its cases.
        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot("SNP0", null, F.asSet(CACHE), true)
            .get();

        checkData(2_000);
    }

    /** */
    @Test
    public void testRecoveryUntilWalEndForEverySnapshot() throws Exception {
        loadData(0, 1_000);

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadData(1_000, 2_000);

        long beforeSecondSnpVer = cutMgr(grid(0)).cutVersion().version();

        grid(0).snapshot().createSnapshot("SNP1").get();

        assertTrue(cutMgr(grid(0)).cutVersion().version() >= beforeSecondSnpVer);

        loadAndAwaitWalArchived(2_000, 3_000, true);

        for (String snp: F.asList("SNP0", "SNP1")) {
            restartWithCleanPersist();

            assertEquals(0, cutMgr(grid(0)).cutVersion().version());

            grid(0).context().cache().context().snapshotMgr()
                .restoreSnapshot(snp, null, null, true)
                .get();

            checkData(3_000);
        }
    }

    // TODO: replace with WARNING. Timestamp? ConsistentCutVersion became too big? Write local timestamp in WAL.
    //  only with ConsistentCutStartRecord / FinishRecord.
    //  Timestamp is needed to decide how to recover failed node - Rebalance or PITR?

    /** */
    @Test
    public void testRecoveryOnSnapshotOnlyIfNoWals() throws Exception {
        loadData(0, 1_000);

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadAndAwaitWalArchived(1_000, 2_000, true);

        restartWithCleanPersist();

        deleteNodesDirs(DFLT_WAL_ARCHIVE_PATH);

        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot("SNP0", null, null, true)
            .get();

        checkData(1_000);
    }

    /** */
    @Test
    public void testRollbackIfNoClusterSnapshotRecordFoundOnSingleNode() throws Exception {
        loadAndAwaitWalArchived(0, 1_000, false);

        grid(0).snapshot().createSnapshot("SNP0").get();

        // Remove 2 segments: ClusterSnapshotRecord belongs one of them.
        long curSeg = walMgr(grid(1)).lastWritePointer().index();
        long archSeg = walMgr(grid(1)).lastArchivedSegment();

        // Need at least 3 segments for test, because 2 are removed.
        while (walMgr(grid(1)).lastArchivedSegment() < 3)
            loadAndAwaitWalArchived(1_000, 1_100, false);

        restartWithCleanPersist();

        String rmFile1 = String.format("000000000000000%d.wal", curSeg);
        String rmFile2 = String.format("000000000000000%d.wal", archSeg);

        deleteNodeFiles(1, DFLT_WAL_ARCHIVE_PATH, rmFile1, rmFile2);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> grid(0).context().cache().context().snapshotMgr()
                    .restoreSnapshot("SNP0", null, null, true)
                    .get()
            , IgniteException.class, "Fail to find ClusterSnapshot record for snapshot SNP0 in WAL");

        assertNull(grid(0).cache(CACHE));
        assertNull(grid(0).cache(CACHE2));
    }

    /** */
    @Test
    public void testRecoveryOnClusterSnapshotIfNoWalsOnSingleNode() throws Exception {
        loadData(0, 1_000);

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadAndAwaitWalArchived(1_000, 2_000, true);

        restartWithCleanPersist();

        deleteNodeFiles(1, DFLT_WAL_ARCHIVE_PATH);

        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot("SNP0", null, null, true)
            .get();

        checkData(1_000);
    }

    /** */
    @Test
    public void testForDifferentConsistentCuts() throws Exception {
        loadData(0, 1_000);

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadData(1_000, 2_000);

        awaitGlobalCutReady(cutMgr(grid(0)).cutVersion().version() + 1, false);

        BlockingConsistentCutManager.cutMgr(grid(1)).block(BlkCutType.AFTER_VERSION_UPDATE);
        BlockingConsistentCutManager.cutMgr(grid(1)).awaitBlocked();

        ConsistentCutVersion blkCutVer = cutMgr(grid(0)).cutVersion();
        awaitGlobalCutVersionReceived(blkCutVer.version(), -1);

        loadAndAwaitWalArchived(2_000, 3_000, false);

        assertEquals(blkCutVer, cutMgr(grid(0)).cutVersion());

        restartWithCleanPersist();

        TestRecordingCommunicationSpi.spi(grid(0)).record(SnapshotOperationRequest.class);

        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot("SNP0", null, null, true)
            .get();

        List<Object> snpReqs = TestRecordingCommunicationSpi.spi(grid(0)).recordedMessages(true);
        for (Object req: snpReqs) {
            long reqVer = ((SnapshotOperationRequest)req).cutVersions().iterator().next().version();
            assertEquals(blkCutVer.version() - 1, reqVer);
        }

        checkData(2_000);
    }

    /** */
    @Test
    public void testFailedOnCorruptedWalSegmentBeforeClusterSnapshot() throws Exception {
        loadData(0, 1_000);

        WALPointer corruptPtr = walMgr(grid(1)).lastWritePointer();

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadAndAwaitWalArchived(1_000, 2_000, true);

        restartWithCleanPersist();

        FileDescriptor[] walArchiveFiles = walMgr(grid(1)).walArchiveFiles();

        WalTestUtils.corruptWalSegmentFile(walArchiveFiles[(int)corruptPtr.index()], corruptPtr);

        GridTestUtils.assertThrows(log,
            () -> grid(0).context().cache().context().snapshotMgr()
                .restoreSnapshot("SNP0", null, null, true)
                .get(),
            IgniteException.class, null
        );

        assertNull(grid(0).cache(CACHE));
        assertNull(grid(0).cache(CACHE2));
    }

    /** */
    @Test
    public void testApplyEarlierCutVersionIfCorruptedWalSegmentAfterClusterSnapshot() throws Exception {
        loadData(0, 1_000);

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadAndAwaitWalArchived(1_000, 2_000, true);

        long cutBeforeCorrupt = cutMgr(grid(0)).cutVersion().version();

        WALPointer corruptPtr = walMgr(grid(1)).lastWritePointer();

        loadAndAwaitWalArchived(2_000, 3_000, true);

        long cutAfterCorrupt = cutMgr(grid(0)).cutVersion().version();

        FileDescriptor[] walArchiveFiles = walMgr(grid(1)).walArchiveFiles();

        WalTestUtils.corruptWalSegmentFile(walArchiveFiles[(int)corruptPtr.index()], corruptPtr);

        restartWithCleanPersist();

        TestRecordingCommunicationSpi.spi(grid(0)).record(SnapshotOperationRequest.class);

        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot("SNP0", null, null, true)
            .get();

        List<Object> snpReqs = TestRecordingCommunicationSpi.spi(grid(0)).recordedMessages(true);

        for (Object req: snpReqs) {
            long recoveryVer = ((SnapshotOperationRequest)req).cutVersions().iterator().next().version();

            assertTrue(recoveryVer >= cutBeforeCorrupt && recoveryVer < cutAfterCorrupt);
        }
    }

    /** */
    @Test
    public void testRecoveryOverRecovery() throws Exception {
        loadData(0, 1_000);

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadAndAwaitWalArchived(1_000, 2_000, true);

        restartWithCleanPersist();

        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot("SNP0", null, null, true)
            .get();

        loadAndAwaitWalArchived(2_000, 3_000, true);

        restartWithCleanPersist();

        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot("SNP0", null, null, true)
            .get();

        checkData(3_000);
    }

    /** */
    @Test
    public void testRecoveryOverRecoveryWithBlockedCut() throws Exception {
        loadData(0, 1_000);

        grid(0).snapshot().createSnapshot("SNP0").get();

        loadAndAwaitWalArchived(1_000, 2_000, true);

        BlockingConsistentCutManager.cutMgr(grid(1)).block(BlkCutType.AFTER_VERSION_UPDATE);
        BlockingConsistentCutManager.cutMgr(grid(1)).awaitBlocked();

        ConsistentCutVersion blkCutVer = cutMgr(grid(0)).cutVersion();
        awaitGlobalCutVersionReceived(blkCutVer.version(), -1);

        loadAndAwaitWalArchived(2_000, 3_000, false);

        restartWithCleanPersist();

        grid(0).context().cache().context().snapshotMgr()
            .restoreSnapshot("SNP0", null, null, true)
            .get();

        loadAndAwaitWalArchived(3_000, 4_000, true);

//        restartWithCleanPersist();
//
//        grid(0).context().cache().context().snapshotMgr()
//            .restoreSnapshot("SNP0", null, null, true)
//            .get();
//
//        checkData(3_000);
    }

    /** */
    private void loadAndAwaitWalArchived(int from, int to, boolean awaitCut) throws Exception {
        loadData(from, to);

        if (awaitCut) {
            long awaitVer = awaitGlobalCutReady(cutMgr(grid(0)).cutVersion().version() + 1, false);

            log.info("Finish await Consistent Cut version = " + awaitVer);

        }

        flushWalAllGrids();

        AtomicInteger nodeId = new AtomicInteger(-1);

        multithreadedAsync(() -> {
            int n = nodeId.incrementAndGet();

            long segNeedToArchive = walMgr(grid(n)).lastWritePointer().index();

            log.info("NODE " + n + " " + segNeedToArchive);

            while (walMgr(grid(n)).lastArchivedSegment() < segNeedToArchive)
                loadData(from, from + 1);

        }, nodes()).get();
    }

    /** */
    private void checkData(int expSize) {
        int size = grid(0).cache(CACHE).query(new ScanQuery<Integer, Integer>()).getAll().size();

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

        for (int part = 0; part < PARTS; part++) {
            VisorConsistencyTaskResult res = grid(0).compute().execute(
                VisorConsistencyRepairTask.class,
                new VisorTaskArgument<>(
                    G.allGrids().stream().map(ign -> ign.cluster().localNode().id()).collect(Collectors.toList()),
                    new VisorConsistencyRepairTaskArg(CACHE, part, ReadRepairStrategy.CHECK_ONLY),
                    false)
            );

            assertFalse(res.message(), res.cancelled());
            assertFalse(res.message(), res.failed());
        }

        assertFalse(readRepairCheckFailed.get());
    }

    /**
     * Stop grids, clean persistence files. Skip deleting WAL archive files only.
     */
    private Map<Integer, WALPointer> restartWithCleanPersist() throws Exception {
        stopAllGrids();

        assertTrue(U.delete(Paths.get(U.defaultWorkDirectory(), "cp").toFile()));

        // Clean all except WAL archive files that are required for recovery.
        deleteNodesDirs(DFLT_STORE_DIR, DFLT_BINARY_METADATA_PATH, DFLT_WAL_PATH);

        moveArchives(false);

        startGrids(nodes());
        grid(0).cluster().state(ClusterState.ACTIVE);

        // Caches are configured with IgniteConiguration., need to destroy them before restoring snapshot.
        grid(0).destroyCaches(F.asList(CACHE, CACHE2));

        Map<Integer, WALPointer> before = new HashMap<>();

        for (int i = 0; i < nodes(); i++) {
            long archSeg = walMgr(grid(i)).lastArchivedSegment();

            walMgr(grid(i)).onDeActivate(grid(i).context());

            before.put(i, new WALPointer(archSeg + 1, 0, 0));
        }

        deleteNodesDirs(DFLT_WAL_ARCHIVE_PATH);

        moveArchives(true);

        return before;
    }

    /** */
    private void moveArchives(boolean restore) throws IgniteCheckedException, IOException {
        Path walBackupDir = Paths.get(U.defaultWorkDirectory(), DFLT_WAL_ARCHIVE_PATH + "0");

        if (!restore && !Files.exists(walBackupDir))
            Files.createDirectory(walBackupDir);

        for (int i = 0; i < nodes(); i++) {
            String nodeFolder = U.maskForFileName(consistentIds.get(i).toString());

            Path walArchive = Paths.get(U.defaultWorkDirectory(), DFLT_WAL_ARCHIVE_PATH, nodeFolder);
            Path walBackup = walBackupDir.resolve(nodeFolder);

            if (restore) {
                System.out.println("BEFORE mv: " + Files.list(walBackup).collect(Collectors.toList()));

                Files.move(walBackup, walArchive);

                System.out.println("After mv: " + Files.list(walArchive).collect(Collectors.toList()));
            }
            else {
                System.out.println("BEFORE mv: " + Files.list(walArchive).collect(Collectors.toList()));

                Files.move(walArchive, walBackup);
            }
        }
    }

    /** */
    private void deleteNodesDirs(String... dirs) throws IgniteCheckedException  {
        for (int i = 0; i < nodes(); i++) {
            String nodeFolder = U.maskForFileName(consistentIds.get(i).toString());

            for (String dir: dirs)
                assertTrue(U.delete(Paths.get(U.defaultWorkDirectory(), dir, nodeFolder).toFile()));
        }
    }

    /** */
    private void deleteNodeFiles(int nodeId, String subDir, String... files) throws IgniteCheckedException  {
        String nodeFolder = U.maskForFileName(consistentIds.get(nodeId).toString());

        if (files.length == 0)
            assertTrue(subDir, U.delete(Paths.get(U.defaultWorkDirectory(), subDir, nodeFolder).toFile()));

        for (String file: files)
            assertTrue(file, U.delete(Paths.get(U.defaultWorkDirectory(), subDir, nodeFolder, file).toFile()));
    }

    /** */
    private void loadData(int from, int to) {
        for (int i = from; i < to; i++) {
            try (Transaction tx = grid(0).transactions().txStart()) {
                grid(0).cache(CACHE).put(i, 0);
                grid(0).cache(CACHE2).put(i, 0);

                tx.commit();
            }
        }
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
