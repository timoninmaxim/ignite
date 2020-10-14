package org.apache.ignite.cache.query.index;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.index.sorted.SortOrder;
import org.apache.ignite.cache.query.index.sorted.SortedIndex;
import org.apache.ignite.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.cache.query.index.sorted.SortedIndexFactory;
import org.apache.ignite.cache.query.index.sorted.SortedIndexFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class IndexTest extends GridCommonAbstractTest {
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

    // TODO: get list of indexes

    /** */
    @Test
    public void testDefaultIndexKey() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, String, Integer> func = new SortedIndexFunction<>(
            (k ,v) -> k, Integer.class
        );

        SortedIndex<Integer, String> idx = createIndex(ignite, func);

        // Check empty index
        assertNull(idx.get(1));

        // Check put item
        cache.put(1, "One");
        assertEquals("One", idx.get(1));

        // Check update item
        cache.put(1, "NewOne");
        assertEquals("NewOne", idx.get(1));

        // Check remove item
        cache.remove(1);
        assertNull(idx.get(1));
    }

    /** */
    @Test
    public void testThatIndexContainsDuplicatedKeys() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, String, String> func = new SortedIndexFunction<>(
            (k ,v) -> v, String.class
        );
        SortedIndex<String, String> idx = createIndex(ignite, func);

        cache.put(1, "One");
        cache.put(2, "One");

        GridCursor<String> cursor = idx.find("One", "One");

        assertTrue(cursor.next());
        assertEquals("One", cursor.get());

        assertTrue(cursor.next());
        assertEquals("One", cursor.get());

        assertFalse(cursor.next());
    }

    /** */
    @Test
    public void testCustomIndexKey() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, String, String> func = new SortedIndexFunction<>(
            (k, v) -> "INDEX" + v, String.class
        );

        SortedIndex<String, String> idx = createIndex(ignite, func);

        // Check empty index
        assertNull(idx.get("One"));

        // Check put item
        cache.put(1, "One");
        assertNull(idx.get("One"));
        assertEquals("One", idx.get("INDEXOne"));

        // Check up date item
        cache.put(1, "NewOne");
        assertNull(idx.get("One"));
        assertNull(idx.get("INDEXOne"));
        assertEquals("NewOne", idx.get("INDEXNewOne"));

        // Check remove item
        cache.remove(1);
        assertNull(idx.get("One"));
        assertNull(idx.get("INDEXOne"));
        assertNull(idx.get("INDEXNewOne"));
    }

    /** */
    @Test
    public void testFindBetween() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, Integer, Integer> func = new SortedIndexFunction<>(
            (k, v) -> k * v, Integer.class
        );
        SortedIndex<Integer, Integer> idx = createIndex(ignite, func);

        // Cache: (0, 0), (1, 1), (2, 2), (3, 3)
        // Index: (0, 0), (1, 1), (4, 2), (9, 3)
        for (int i = 0; i < 4; i ++)
            cache.put(i, i);

        assertEquals(0, idx.get(0).intValue());
        assertEquals(1, idx.get(1).intValue());
        assertEquals(2, idx.get(4).intValue());
        assertEquals(3, idx.get(9).intValue());

        // Check limited bounds
        GridCursor<Integer> result = idx.find(-1, 10);
        assertTrue(result.next());
        assertEquals(0, result.get().intValue());

        assertTrue(result.next());
        assertEquals(1, result.get().intValue());

        assertTrue(result.next());
        assertEquals(2, result.get().intValue());

        assertTrue(result.next());
        assertEquals(3, result.get().intValue());

        assertFalse(result.next());

        // Check left unbound
        result = idx.find(null, 10);
        assertTrue(result.next());
        assertEquals(0, result.get().intValue());

        assertTrue(result.next());
        assertEquals(1, result.get().intValue());

        assertTrue(result.next());
        assertEquals(2, result.get().intValue());

        assertTrue(result.next());
        assertEquals(3, result.get().intValue());

        assertFalse(result.next());

        // Check right unbound
        result = idx.find(0, null);
        assertTrue(result.next());
        assertEquals(0, result.get().intValue());

        assertTrue(result.next());
        assertEquals(1, result.get().intValue());

        assertTrue(result.next());
        assertEquals(2, result.get().intValue());

        assertTrue(result.next());
        assertEquals(3, result.get().intValue());

        assertFalse(result.next());

        // Check internal search
        result = idx.find(2, 4);

        assertTrue(result.next());
        assertEquals(2, result.get().intValue());

        assertFalse(result.next());
    }

    @Test
    public void testIndexSortOrder() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("CACHE");

        // Ascending order.
        SortedIndexFunction<Integer, Integer, Integer> func = new SortedIndexFunction<>(
            (k, v) -> k, Integer.class
        );
        SortedIndex<Integer, Integer> idx = createIndex(ignite, func);

        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);

        GridCursor<Integer> cursor = idx.find(2, null);

        assertTrue(cursor.next());
        assertEquals(2, cursor.get().intValue());

        assertTrue(cursor.next());
        assertEquals(3, cursor.get().intValue());

        assertFalse(cursor.next());

        // Descending order.
        func = new SortedIndexFunction<>((k, v) -> k, Integer.class, SortOrder.DESC);

        idx = createIndex(ignite, func);

        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);

        cursor = idx.find(2, null);

        assertTrue(cursor.next());
        assertEquals(2, cursor.get().intValue());

        assertTrue(cursor.next());
        assertEquals(1, cursor.get().intValue());

        assertFalse(cursor.next());
    }

    /** */
    protected <CK, CV, IK extends Comparable<IK>> SortedIndex<IK, CV> createIndex(IgniteEx node, SortedIndexFunction<CK, CV, IK> keyFunc) {
        GridCacheContext<CK, CV> cctx = (GridCacheContext<CK, CV>) node.cachex("CACHE").context();

        SortedIndexDefinition def =
            new SortedIndexDefinition(cctx, "idx", 1, keyFunc);

        return (SortedIndex<IK, CV>) idx
            .createIndex(new SortedIndexFactory(), def)
            .unwrap(SortedIndex.class);
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
