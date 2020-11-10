package org.apache.ignite.cache.query.index;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.index.inline.InlineIndexFactory;
import org.apache.ignite.cache.query.index.multi.MultiSortedIndex;
import org.apache.ignite.cache.query.index.sorted.Condition;
import org.apache.ignite.cache.query.index.sorted.SortedIndexDefinition;
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

// TODO: add check that get compare using only inlined fields (check that cache is clear?)
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

    @Test
    public void testFindSingleFunction() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, String, Integer> func1 = new SortedIndexFunction<>(
            (k ,v) -> k, Integer.class
        );

        MultiSortedIndex<String> idx = createIndex(ignite, func1);

        cache.put(1, "One");
        cache.put(2, "Two");
        cache.put(3, "Three");
        cache.put(4, "Four");

        //
        GridCursor<String> cursor = idx.find(new Condition[]{new Condition(-1, 0)});

        assertFalse(cursor.next());

        //
        cursor = idx.find(new Condition[]{new Condition(0, 1)});

        assertTrue(cursor.next());
        assertEquals("One", cursor.get());
        assertFalse(cursor.next());

        //
        cursor = idx.find(new Condition[]{new Condition(4, 5)});

        assertTrue(cursor.next());
        assertEquals("Four", cursor.get());
        assertFalse(cursor.next());

        //
        cursor = idx.find(new Condition[]{new Condition(2, 5)});

        assertTrue(cursor.next());
        assertEquals("Two", cursor.get());
        assertTrue(cursor.next());
        assertEquals("Three", cursor.get());
        assertTrue(cursor.next());
        assertEquals("Four", cursor.get());
        assertFalse(cursor.next());
    }

    @Test
    public void testFindMultipleFunction() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, String, Integer> func1 = new SortedIndexFunction<>(
            (k ,v) -> k, Integer.class
        );

        SortedIndexFunction<Integer, String, Integer> func2 = new SortedIndexFunction<>(
            (k ,v) -> k + 1, Integer.class
        );

        MultiSortedIndex<String> idx = createIndex(ignite, func1, func2);

        cache.put(1, "One");
        cache.put(2, "Two");
        cache.put(3, "Three");
        cache.put(4, "Four");

        //
        GridCursor<String> cursor = idx.find(new Condition[]{new Condition(-1, 0)});

        assertFalse(cursor.next());

        //
        cursor = idx.find(new Condition[]{new Condition(0, 1)});

        assertTrue(cursor.next());
        assertEquals("One", cursor.get());
        assertFalse(cursor.next());

        //
        cursor = idx.find(new Condition[]{new Condition(4, 5)});

        assertTrue(cursor.next());
        assertEquals("Four", cursor.get());
        assertFalse(cursor.next());

        //
        cursor = idx.find(new Condition[]{new Condition(2, 5)});

        assertTrue(cursor.next());
        assertEquals("Two", cursor.get());
        assertTrue(cursor.next());
        assertEquals("Three", cursor.get());
        assertTrue(cursor.next());
        assertEquals("Four", cursor.get());
        assertFalse(cursor.next());
    }

    @Test
    public void testStrings() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("CACHE");

        SortedIndexFunction<Integer, String, String> func1 = new SortedIndexFunction<>(
            (k ,v) -> v, String.class
        );

        MultiSortedIndex<String> idx = createIndex(ignite, func1);

        // Check empty index.
        assertNull(idx.get(new Object[]{ "One" }));

        // Check put item
        cache.put(1, "One");
        cache.put(2, "Two");

        assertEquals("One", idx.get(new Object[]{ "One" }));
        assertEquals("Two", idx.get(new Object[]{ "Two" }));

        // Check update item
        cache.put(1, "NewOne");
        assertNull(idx.get(new Object[]{ "One" }));
        assertEquals("NewOne", idx.get(new Object[]{ "NewOne" }));
        assertEquals("Two", idx.get(new Object[]{ "Two" }));

        // Check remove item
        cache.remove(1);
        assertNull(idx.get(new Object[]{ "NewOne" }));
        assertEquals("Two", idx.get(new Object[]{ "Two" }));
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
