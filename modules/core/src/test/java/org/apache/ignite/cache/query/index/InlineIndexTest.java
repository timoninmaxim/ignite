package org.apache.ignite.cache.query.index;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.index.inline.InlineIndexFactory;
import org.apache.ignite.cache.query.index.multi.MultiSortedIndex;
import org.apache.ignite.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.cache.query.index.sorted.SortedIndexFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InlineIndexTest extends GridCommonAbstractTest {
    /** */
    protected IgniteIndexing idx;

    /** */
    @Before
    public void setUp() throws Exception {
        idx = new IgniteIndexing();
    }

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();
    }

    @Test
    public void testSingleIndexFunction() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, String, Integer> func = new SortedIndexFunction<>(
            (k ,v) -> k, Integer.class
        );
        MultiSortedIndex<String> idx = createIndex(ignite, func);

        // Check empty index
        assertNull(idx.get(new Object[]{ 1 }));

        // Check put item
        cache.put(1, "One");
        cache.put(2, "Two");

        assertEquals("One", idx.get(new Object[]{ 1 }));
        assertEquals("Two", idx.get(new Object[]{ 2 }));

        // Check update item
        cache.put(1, "NewOne");
        assertEquals("NewOne", idx.get(new Object[]{ 1 }));
        assertEquals("Two", idx.get(new Object[]{ 2 }));

        // Check remove item
        cache.remove(1);
        assertNull(idx.get(new Object[]{ 1 }));
        assertEquals("Two", idx.get(new Object[]{ 2 }));
    }

    @Test
    public void testMultipleIndexFunction() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, String, Integer> func1 = new SortedIndexFunction<>(
            (k ,v) -> k, Integer.class
        );

        SortedIndexFunction<Integer, String, Integer> func2 = new SortedIndexFunction<>(
            (k ,v) -> k + 1, Integer.class
        );

        MultiSortedIndex<String> idx = createIndex(ignite, func1, func2);

        // Check empty index
        // TODO: add test for expecting exception
        //assertNull(idx.get(new Object[]{ 1 }));

        assertNull(idx.get(new Object[]{ 1, 1 }));
        assertNull(idx.get(new Object[]{ 1, 2 }));

        // Check put item
        cache.put(1, "One");

        assertEquals(null, idx.get(new Object[]{ 1, 1 }));
        assertEquals("One", idx.get(new Object[]{ 1, 2 }));

        // Check update item
        cache.put(1, "NewOne");

        assertEquals(null, idx.get(new Object[]{ 1, 1 }));
        assertEquals("NewOne", idx.get(new Object[]{ 1, 2 }));

        // Check remove item
        cache.remove(1);

        assertEquals(null, idx.get(new Object[]{ 1, 1 }));
        assertEquals(null, idx.get(new Object[]{ 1, 2 }));
    }

    /** */
    protected <CV> MultiSortedIndex<CV> createIndex(
        IgniteEx node, SortedIndexFunction... keyFunc) {
        GridCacheContext cctx = node.cachex("CACHE").context();

        SortedIndexDefinition def =
            new SortedIndexDefinition(cctx, "idx", 1, keyFunc);

        return (MultiSortedIndex<CV>) idx
            .createIndex(new InlineIndexFactory(), def)
            .unwrap(MultiSortedIndex.class);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setIndexingSpi(idx);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName("CACHE")
            .setQueryParallelism(1));

        return cfg;
    }

}
