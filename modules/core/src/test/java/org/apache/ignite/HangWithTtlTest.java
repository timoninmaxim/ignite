package org.apache.ignite;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 *
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT, value = "30000")
public class HangWithTtlTest extends GridCommonAbstractTest {
    /**
     * Keys count.
     */
    public static final int KEYS_CNT = 1024;

    /**
     * Log listener.
     */
    private final LogListener lsnr = LogListener
        .matches("Possible starvation in striped pool")
        .build();

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg.isClientMode() ?
            cfg :
            cfg.setGridLogger(new ListeningTestLogger(log, lsnr))
                .setDataStorageConfiguration(new DataStorageConfiguration()
                    .setCheckpointFrequency(1000)
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setPersistenceEnabled(true)))
                .setStripedPoolSize(16)
                .setFailureDetectionTimeout(10_000)
                .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setBackups(1)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                    .setExpiryPolicyFactory(TouchedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 10))));
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        lsnr.reset();
    }

    /**
     *
     */
    @Test
    public void testTouchSingleKey() throws Exception {
        test(cache -> cache.put(0, new byte[128]), () -> 0);
    }

    /**
     *
     */
    @Test
    public void testTouchRandomKeys() throws Exception {
        test(this::putAll, () -> ThreadLocalRandom.current().nextInt(KEYS_CNT));
    }

    /**
     *
     */
    private void test(Consumer<IgniteCache<Object, Object>> putAction, IntSupplier keySupplier) throws Exception {
        startGrids(3).cluster().state(ClusterState.ACTIVE);

        IgniteEx client = startClientGrid(3);

        IgniteCache<Object, Object> clientCache = client.cache(DEFAULT_CACHE_NAME);

        putAction.accept(clientCache);

        AtomicBoolean doTest = new AtomicBoolean(true);
        AtomicLong getCnt = new AtomicLong();

        IgniteInternalFuture<Object> logFut = startQueueLogging(doTest, getCnt);

        IgniteInternalFuture<Long> getFut = runMultiThreadedAsync(
            () -> {
                while (doTest.get()) {
                    clientCache.get(keySupplier.getAsInt());

                    getCnt.getAndIncrement();
                }
            },
            16,
            "test-get");

        boolean hasStarvation = lsnr.check(60_000);

        if (hasStarvation)
            log.warning(">>>>>> Starvation after gets: getCnt=" + getCnt.get());

        lsnr.reset();

        doTest.set(false);
        logFut.get();
        getFut.get();

        // Wait for stuck node to revive
        if (hasStarvation)
            assertFalse(lsnr.check(30_000));
    }

    /**
     *
     */
    private IgniteInternalFuture<Object> startQueueLogging(AtomicBoolean doTest, AtomicLong cnt) {
        List<IntMetric> queueMetrics = G.allGrids().stream()
            .filter(ign -> !ign.configuration().isClientMode())
            .map(ign -> ((IgniteEx)ign).context()
                .metric()
                .find("threadPools.StripedExecutor.TotalQueueSize", IntMetric.class))
            .collect(Collectors.toList());

        return runAsync(() -> {
            while (doTest.get()) {
                log.warning(">>>>>> Summary: [cnt=" + cnt.get() + ", queues=" +
                    queueMetrics.stream()
                        .map(IntMetric::value)
                        .collect(Collectors.toList()) + "]");

                doSleep(5000);
            }
        });
    }

    /**
     *
     */
    private void putAll(IgniteCache<Object, Object> clientCache) {
        Map<Integer, byte[]> data = new HashMap<>(KEYS_CNT);

        byte[] val = new byte[128];

        for (int i = 0; i < KEYS_CNT; i++)
            data.put(i, val);

        clientCache.putAll(data);
    }
}
