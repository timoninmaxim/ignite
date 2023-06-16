package org.apache.ignite.compatibility.persistence;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Test;

/** */
public class BugTst extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    private static final String CACHE = "GlobalIndex_1";

    /** */
    private static final Serializable CONSISTENT_ID = "ConsistentId1";

    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        patchConfiguration(cfg);

        return cfg;
    }

    /** {@inheritDoc} */
    public static IgniteConfiguration patchConfiguration(IgniteConfiguration cfg) {
        cfg.setCacheConfiguration(new CacheConfiguration()
            .setName(CACHE)
            .setBackups(1)
            .setGroupName("INDEX_CACHE_GROUP")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setKeyConfiguration(new CacheKeyConfiguration(GlobalIndexKey.class))
            .setQueryEntities(F.asList(
                new QueryEntity(GlobalIndexKey.class, GlobalIndexValue.class)
                    .setFields(new LinkedHashMap<>(F.asMap(
                        "value", "java.lang.String",
                        "payload", "java.lang.Object")))
                    .setKeyFields(F.asSet("value", "payload"))
                    .setIndexes(F.asList(new QueryIndex("value")))
            )));

        cfg.setConsistentId(CONSISTENT_ID);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
    }

    @Test
    public void test() throws Exception {
        try {
            startGrid(1, "2.10.0", new ConfigurationClosure(true), new PostStartupClosure());

            stopAllGrids();

            IgniteEx igniteEx = startGrid(0);

            igniteEx.cluster().state(ClusterState.ACTIVE);

            int idxSize = igniteEx.cache(CACHE).size();

            System.out.println("RESULT " + idxSize);


            multithreadedAsync(() -> {
                Random rnd = ThreadLocalRandom.current();

                for (int i = 0; i < 1_000_000; i++) {
                    int val = 500_000 + rnd.nextInt(1_000);

                    IgniteCache c = igniteEx.cache(CACHE);

                    if (rnd.nextBoolean())
                        c.putAsync(new GlobalIndexKey(String.valueOf(val), i), new GlobalIndexValue());
                    else
                        c.removeAsync(new GlobalIndexKey(String.valueOf(val), i));
                }
            }, 4).get();

            igniteEx.cluster().state(ClusterState.INACTIVE);
        }
        finally {
            stopAllGrids();
        }

    }

    /** */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            try (IgniteDataStreamer<GlobalIndexKey, GlobalIndexValue> s = ignite.dataStreamer(CACHE)) {
                for (int i = 0; i < 1_000_000; i++)
                    s.addData(new GlobalIndexKey(String.valueOf(i), i), new GlobalIndexValue());
            }

            System.out.println("RESULT " + ignite.cache(CACHE).size());

            ignite.active(false);
        }
    }

    /** */
    public static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** Compact footer. */
        private boolean compactFooter;

        /**
         * @param compactFooter Compact footer.
         */
        public ConfigurationClosure(boolean compactFooter) {
            this.compactFooter = compactFooter;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(false);

            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

            cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(compactFooter));

            patchConfiguration(cfg);
        }
    }

    /** */
    public static class GlobalIndexKey {
        @AffinityKeyMapped
        @QuerySqlField(index = true)
        private String value;

        @QuerySqlField
        private Object payload;

        public GlobalIndexKey(String value, Object payload) {
            this.value = value;
            this.payload = payload;
        }
    }

    /** */
    public static class GlobalIndexValue {
    }
}
