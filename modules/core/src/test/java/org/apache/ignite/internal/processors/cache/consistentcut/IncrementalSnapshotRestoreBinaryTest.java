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
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.binary.BinaryClassDescriptor;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** */
public class IncrementalSnapshotRestoreBinaryTest extends AbstractConsistentCutBlockingTest {
    /** */
    @Test
    public void testMetaRestoredAndAlivedAfterRestart() throws Exception {
        loadData();

        snp(grid(0)).createSnapshot(SNP, null, true).get();

        restartWithCleanPersistence();

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData();

        // Restart.
        stopAllGrids();
        startGrids(nodes());

        checkData();
    }

    /** */
    @Test
    public void testMarshaller() throws Exception {
        BinaryContext binCtx = ((CacheObjectBinaryProcessorImpl)grid(0).context().cacheObjects()).binaryContext();

        BinaryClassDescriptor persClsDesc = binCtx.registerClass(Person.class, true, false);
        binCtx.registerDescriptor(persClsDesc, true, false);
        binCtx.registerClassLocally(binCtx.metadata(persClsDesc.typeId()), true, (byte)1);

        Consumer<Boolean> check = (empty) -> {
            ArrayList<Map<Integer, MappedName>> mappings = grid(0).context().marshallerContext().getCachedMappings();

            if (empty) {
                assertEquals(mappings.toString(), 1, mappings.size());
                assertTrue(mappings.get(0).isEmpty());
            }
            else {
                assertEquals(mappings.toString(), 2, mappings.size());
                assertTrue(mappings.get(0).containsKey(persClsDesc.typeId()));
                assertTrue(mappings.get(1).containsKey(persClsDesc.typeId()));
            }
        };

        check.accept(false);

        snp(grid(0)).createSnapshot(SNP, null, true).get();

        restartWithCleanPersistence();

        check.accept(true);

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        check.accept(false);

        stopAllGrids();

        startGrids(nodes());

        check.accept(false);
    }

    /** */
    @Test
    public void testBinaryCreatedOnSingleNodeOnlyBeforeSnapshot() throws Exception {
        loadData();

        snp(grid(0)).createSnapshot(SNP, null, true).get();

        restartWithCleanPersistence();

        // Delete binary meta from incremental snapshot.
        File incSnpDir = snp(grid(0)).incrementalSnapshotLocalDir(SNP, null, 1);
        assertTrue(U.delete(new File(incSnpDir, DataStorageConfiguration.DFLT_BINARY_METADATA_PATH)));

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        GridTestUtils.assertThrows(log, () -> {
            Person p = (Person)grid(0).cache(CACHE).get(1);
        }, IgniteException.class, "Cannot find metadata for object");
    }

    /** Prepare for snapshot restoring - restart grids, with clean persistence. */
    private void restartWithCleanPersistence() throws Exception {
        stopAllGrids();

        assertTrue(U.delete(Paths.get(U.defaultWorkDirectory(), "cp").toFile()));
        assertTrue(U.delete(Paths.get(U.defaultWorkDirectory(), DFLT_MARSHALLER_PATH).toFile()));

        deleteNodesDirs(DFLT_STORE_DIR, DFLT_BINARY_METADATA_PATH, DFLT_WAL_PATH, DFLT_WAL_ARCHIVE_PATH);

        startGrids(nodes());

        grid(0).cluster().state(ClusterState.ACTIVE);

        // Caches are configured with IgniteConiguration, need to destroy them before restoring snapshot.
        grid(0).destroyCaches(F.asList("CACHE"));

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
    private void loadData() {
        for (int i = 0; i < 1_000; i++) {
            try (Transaction tx = grid(0).transactions().txStart()) {
                grid(0).cache(CACHE).put(i, new Person("pers" + i));

                tx.commit();
            }
        }
    }

    /** */
    private void checkData() {
        for (int i = 0; i < 1000; i++) {
            Person p = (Person)grid(0).cache(CACHE).get(i);

            assertEquals("pers" + i, p.name);
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

    /** */
    private static class Person {
        /** */
        private final String name;

        /** */
        Person(String name) {
            this.name = name;
        }
    }
}
