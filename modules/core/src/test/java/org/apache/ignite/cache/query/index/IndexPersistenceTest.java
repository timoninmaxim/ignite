package org.apache.ignite.cache.query.index;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.index.sorted.SortedIndex;
import org.apache.ignite.cache.query.index.sorted.SortedIndexFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** */
public class IndexPersistenceTest extends IndexTest {
    /** */
    @Before
    @Override public void setUp() throws Exception {
        super.setUp();
        cleanPersistenceDir();
    }

    /** */
    @After
    @Override public void tearDown() throws Exception {
        super.tearDown();
        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testRestoreFromPDS() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, String, Integer> func = new SortedIndexFunction<>(
            (k ,v) -> k, Integer.class
        );
        SortedIndex<Integer, String> idx = createIndex(ignite, func);

        cache.put(1, "One");
        assertEquals("One", idx.get(1));

        stopGrid();

        ignite = startGrid();

        cache = ignite.getOrCreateCache("CACHE");
        idx = createIndex(ignite, func);

        assertEquals("One", cache.get(1));
        assertEquals("One", idx.get(1));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid() throws Exception {
        IgniteEx ignite = super.startGrid();
        ignite.cluster().state(ClusterState.ACTIVE);

        return ignite;
    }
}
