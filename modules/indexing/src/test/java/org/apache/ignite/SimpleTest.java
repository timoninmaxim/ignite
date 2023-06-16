package org.apache.ignite;

import java.util.LinkedHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/** */
@WithSystemProperty(key = "IGNITE_BPLUS_TREE_LOCK_RETRIES", value = "100")
public class SimpleTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "GlobalIndex_1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
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
            )));

        cfg.setDataStreamerThreadPoolSize(8);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setCheckpointFrequency(15_000)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @Test
    public void test() throws Exception {
        IgniteEx crd = startGrids(4);

        crd.cluster().state(ClusterState.ACTIVE);

        LongAdder cntCommit = new LongAdder();
        LongAdder cntRollback = new LongAdder();

        try (IgniteDataStreamer<GlobalIndexKey, GlobalIndexValue> streamer = crd.dataStreamer(CACHE)) {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < 5_000_000; i++) {
                if (i % 1000 == 0)
                    System.out.println("LOADED " + i + " entries");

                streamer.addData(key(rnd), new GlobalIndexValue());
            }
        }

        IgniteInternalFuture f = multithreadedAsync(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            try (IgniteClient cln = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1"))) {
                while (true) {
                    if (cntCommit.sum() % 1_000 == 0)
                        System.out.println("COMMIT " + cntCommit.sum() + ", ROLLBACK " + cntRollback.sum());

                    ClientCache<GlobalIndexKey, GlobalIndexValue> cache = cln.cache(CACHE);

                    try (ClientTransaction tx = cln.transactions().txStart(OPTIMISTIC, TransactionIsolation.SERIALIZABLE)) {
                        cache.put(key(rnd), new GlobalIndexValue());

                        tx.commit();

                        cntCommit.increment();
                    }
                    catch (Exception e) {
                        cntRollback.increment();
                    }
                }
            }

        }, 4);

        f.get();
    }

    /** */
    private GlobalIndexKey key(ThreadLocalRandom rnd) {
        String val = rnd.nextInt(1000) + "000000";
        Person pers = new Person(val, rnd.nextInt(1_000_000), rnd.nextInt());

        return new GlobalIndexKey(val, pers);
    }

    /** */
    public static class Person {
        /** */
        private String value;

        private int salary;

        private int id;

        public Person(String val, int salary, int id) {
            this.value = val;
            this.salary = salary;
            this.id = id;
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
