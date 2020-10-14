package org.apache.ignite.index;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.index.IgniteIndexing;
import org.apache.ignite.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.cache.query.index.sorted.SortedIndexFunction;
import org.apache.ignite.cache.query.index.sorted.SortedIndex;
import org.apache.ignite.cache.query.index.sorted.SortedIndexFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexTest extends GridCommonAbstractTest {

    private IgniteIndexing idx;

    @Before
    public void setUp() {
        idx = new IgniteIndexing();
    }

    @After
    public void tearDown() {
        stopAllGrids();
    }

    @Test
    public void testAddToCacheAddToIndex() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache cache = ignite.getOrCreateCache("CACHE");
        SortedIndex index = createIndex(ignite);

        cache.put(1, "One");
        cache.put(2, "Two");

        assertEquals("One", index.get(1));
        assertEquals("Two", index.get(2));
    }


    @Test
    public void testIndexIsBuildFromCache() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache cache = ignite.getOrCreateCache("CACHE");

        cache.put(1, "One");
        cache.put(2, "Two");

        SortedIndex index = createIndex(ignite);

        Thread.sleep(1000);

        assertEquals("One", index.get(1));
        assertEquals("Two", index.get(2));
    }

    @Test
    public void queryIndexCreate() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache cache = ignite.getOrCreateCache("CACHE");

        cache.put(1, "One");
        cache.put(2, "Two");

        SqlFieldsQuery q = new SqlFieldsQuery("select id, name from SCHEMA.CACHE");

        List result = cache.query(q).getAll();
        for (Object o: result)
            System.out.println(o);
    }

    private SortedIndex createIndex(IgniteEx node) {
        GridCacheContext cctx = node.cachex("CACHE").context();

        SortedIndexFunction<Integer, String> func = (k, v) -> k.hashCode();

        SortedIndexDefinition def = new SortedIndexDefinition(cctx, func, "idx", 1);

        return idx
            .createIndex(new SortedIndexFactory(), def)
            .unwrap(SortedIndex.class);
    }

    private int hash(CacheDataRow value) {
        return value.hash();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setIndexingSpi(idx);

        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>();
        ccfg.setName("CACHE");
        ccfg.setSqlSchema("SCHEMA");
        ccfg.setQueryParallelism(1);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", Integer.class.getName());
        fields.put("name", String.class.getName());

        QueryEntity entity = new QueryEntity()
            .setTableName("CACHE")
            .setKeyType(Integer.class.getName())
            .setKeyFieldName("id")
            .setValueType(String.class.getName())
            .setValueFieldName("name")
            .setFields(fields);

        ccfg.setQueryEntities(Collections.singletonList(entity));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }
}
