///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.ignite.internal.processors.cache.consistentcut;
//
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.Random;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Collectors;
//import javax.cache.Cache;
//import org.apache.ignite.cache.CacheAtomicityMode;
//import org.apache.ignite.cache.ReadRepairStrategy;
//import org.apache.ignite.cache.query.QueryCursor;
//import org.apache.ignite.cache.query.ScanQuery;
//import org.apache.ignite.cluster.ClusterState;
//import org.apache.ignite.cluster.ClusterTopologyException;
//import org.apache.ignite.configuration.CacheConfiguration;
//import org.apache.ignite.configuration.DataRegionConfiguration;
//import org.apache.ignite.configuration.DataStorageConfiguration;
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.apache.ignite.internal.IgniteInternalFuture;
//import org.apache.ignite.internal.NodeStoppingException;
//import org.apache.ignite.internal.util.typedef.G;
//import org.apache.ignite.internal.util.typedef.internal.U;
//import org.apache.ignite.internal.visor.VisorTaskArgument;
//import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTask;
//import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTaskArg;
//import org.apache.ignite.internal.visor.consistency.VisorConsistencyTaskResult;
//import org.apache.ignite.transactions.Transaction;
//import org.junit.Test;
//
//import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;
//
//// TODO:
////  1. ClusterSnapshotRecoveryRecord (additionally to ClusterSnapshotRecord)?
////      after we recover on Snapshot -- we start writing WALs from scratch?
////      recovery + archive WALs + | enable WAL here | start writing WAL after recovery | + start new CUTS. Which version?
////      Setup initial ConsistentCut version in ConsistentCutManager from WAL.
////      BUT How to merge WAL archived from PAST with new WAL archived after recovery? Should be possible?
////          In case we need to recover again after some time after previous recovery.
////
////  2.
//
//// TODO:
////  1. Restart --> basline
//
///** */
//public class PITRDemo extends AbstractConsistentCutTest {
//    /** */
//    private final AtomicInteger key = new AtomicInteger();
//
//    /** {@inheritDoc} */
//    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
//        IgniteConfiguration cfg = super.getConfiguration(instanceName);
//
//        cfg.setCacheConfiguration(
//            new CacheConfiguration<Integer, Integer>()
//                .setName(CACHE)
//                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
//                .setBackups(2)
//                .setDataRegionName("consistent-cut-persist"));
//
//        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
//            .setPitrEnabled(true)
//            .setPitrPeriod(1_000)  // 1 sec.
//            .setWalSegmentSize(2 * 1024 * 1024)  // 2Mb, enforce archiving segments.
//            .setDataRegionConfigurations(new DataRegionConfiguration()
//                .setName("consistent-cut-persist")
//                .setPersistenceEnabled(true)));
//
//        cfg.setIncludeEventTypes(EVT_CONSISTENCY_VIOLATION);
//
//        return cfg;
//    }
//
//    /** */
//    @Test
//    public void test() throws Exception {
//        startGrids(nodes());
//
//        grid(0).cluster().state(ClusterState.ACTIVE);
//
//        IgniteInternalFuture<?> loadFut = loadAsync();
//
//        try {
//            Thread.sleep(5_000);
//
//            assertTrue(TestConsistentCutManager.disabled(grid(0)));
//
//            grid(0).snapshot().createSnapshot("SNP0").get();
//
//            assertFalse(TestConsistentCutManager.disabled(grid(0)));
//
//            // Key that is equal or greater than last key included into snapshot.
//            int afterSnpKey = key.get();
//
//            Thread.sleep(5_000);
//
//            stopAllGrids();
//
//            loadFut.cancel();
//
//            for (int i = 0; i < nodes(); i++)
//                cleanPersistenceDir(getTestIgniteInstanceName(i));
//
//            Path walPath = Paths.get(U.defaultWorkDirectory()).resolve(DataStorageConfiguration.DFLT_WAL_PITR_PATH);
//
//            Path backWalPath = Paths.get(U.defaultWorkDirectory()).resolve(DataStorageConfiguration.DFLT_WAL_PITR_PATH + "_backup");
//            Files.createDirectory(backWalPath);
//
//            Files.move(walPath, backWalPath.resolve("0"));
//
//            startGrids(nodes());
//
//            grid(0).cluster().state(ClusterState.ACTIVE);
//
//            assertTrue(TestConsistentCutManager.disabled(grid(0)));
//
//            grid(0).destroyCache(CACHE);
//
//            grid(0).context().cache().context().snapshotMgr()
//                .restoreSnapshot("SNP0", null, backWalPath.toString(), null)
//                .get();
//
//            assertFalse(TestConsistentCutManager.disabled(grid(0)));
//
//            // Data after snapshot recovered.
//            QueryCursor<Cache.Entry<Integer, Integer>> afterSnpCursor = grid(0).cache(CACHE).query(
//                new ScanQuery<Integer, Integer>().setFilter((k, v) -> k > afterSnpKey)
//            );
//
//            assertTrue(afterSnpCursor.iterator().hasNext());
//
//            // Idle verify - OK.
//            for (int i = 0; i < nodes(); i++)
//                idleVerify(grid(i));
//
//            // Read repair check - OK.
//            for (int i = 0; i < 1; i++) {
//                VisorConsistencyTaskResult res = grid(0).compute().execute(
//                    VisorConsistencyRepairTask.class,
//                    new VisorTaskArgument<>(
//                        G.allGrids().stream().map(ign -> ign.cluster().localNode().id()).collect(Collectors.toList()),
//                        new VisorConsistencyRepairTaskArg(CACHE, 0, ReadRepairStrategy.CHECK_ONLY),
//                        false)
//                );
//
//                assertFalse(res.cancelled());
//                assertFalse(res.failed());
//            }
//        }
//        finally {
//            loadFut.cancel();
//        }
//    }
//
//    /** */
//    private void loadAndSnapshotConcurrently() throws Exception {
//        CountDownLatch latch = new CountDownLatch(1);
//
//        AtomicInteger key = new AtomicInteger();
//
//        IgniteInternalFuture<?> f = multithreadedAsync(() -> {
//            while (latch.getCount() > 0)
//                grid(0).cache(CACHE).put(key.getAndIncrement(), 0);
//        }, 1);
//
//        for (int i = 0; i < 1; i++) {
//            Thread.sleep(1_000);
//
//            grid(0).snapshot().createSnapshot("SNP_" + i).get();
//        }
//
//        latch.countDown();
//
//        f.get();
//    }
//
//    /** */
//    private IgniteInternalFuture<?> loadAsync() throws Exception {
//        return multithreadedAsync(() -> {
//            Random rnd = new Random();
//
//            while (true) {
//                try {
//                    int nearNode = rnd.nextInt(nodes());
//                    int keys = rnd.nextInt(nodes());
//
//                    try (Transaction tx = grid(nearNode).transactions().txStart()) {
//                        for (int i = 0; i < keys; i++)
//                            grid(nearNode).cache(CACHE).put(key.incrementAndGet(), 0);
//
//                        tx.commit();
//                    }
//                }
//                catch (Exception e) {
//                    if (e.getCause() instanceof ClusterTopologyException || e.getCause() instanceof NodeStoppingException)
//                        break;
//
//                    throw e;
//                }
//            }
//        }, 1);
//    }
//
//    /** {@inheritDoc} */
//    @Override protected int nodes() {
//        return 4;
//    }
//
//    /** {@inheritDoc} */
//    @Override protected int backups() {
//        return 2;
//    }
//
//    /** {@inheritDoc} */
//    @Override protected void beforeTest() throws Exception {
//        cleanPersistenceDir();
//    }
//
//    /** {@inheritDoc} */
//    @Override protected void afterTest() throws Exception {
//        stopAllGrids();
//    }
//}
