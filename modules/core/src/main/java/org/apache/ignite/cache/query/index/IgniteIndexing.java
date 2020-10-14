package org.apache.ignite.cache.query.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.index.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.cache.query.index.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.jetbrains.annotations.Nullable;


// TODO: IndexManager?
public class IgniteIndexing implements IndexingSpi {

    static {
        AbstractInlineInnerIO.register();
        AbstractInlineLeafIO.register();
    }

    private final Map<String, List<Index>> cacheToIdx = new HashMap<>();

    @Override public Iterator<Cache.Entry<?, ?>> query(@Nullable String cacheName, Collection<Object> params,
        @Nullable IndexingQueryFilter filters) throws IgniteSpiException {
        return null;
    }

    @Override public void store(@Nullable String cacheName, Object key, Object val,
        long expirationTime) throws IgniteSpiException {

    }

    @Override public void store(GridCacheContext cctx, CacheDataRow newRow, @Nullable CacheDataRow prevRow,
        boolean prevRowAvailable) throws IgniteSpiException {

        String cacheName = cctx.cache().name();
        List<Index> indexes = cacheToIdx.get(cacheName);

        if (indexes == null)
            return;

        try {
            for (Index idx: indexes)
                idx.onUpdate(prevRow, newRow);

        } catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to update indexes.", e);
        }
    }

    @Override public void remove(@Nullable String cacheName, Object key) throws IgniteSpiException {
    }

    @Override public void remove(String cacheName, @Nullable CacheDataRow prevRow) throws IgniteSpiException {
        List<Index> indexes = cacheToIdx.get(cacheName);

        if (indexes == null)
            return;

        try {
            for (Index idx: indexes)
                idx.onUpdate(prevRow, null);

        } catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to remove item.", e);
        }
    }

    /** */
    // TODO: send event of schema change?
    @Override public Index createIndex(IndexFactory factory, IndexDefinition definition) {
        Index idx = factory.createIndex(definition);
        String cacheName = definition.getContext().cache().name();

        if (!cacheToIdx.containsKey(cacheName))
            cacheToIdx.put(cacheName, new ArrayList<>());

        cacheToIdx.get(cacheName).add(idx);

        return idx;
    }


















    @Override public String getName() {
        return null;
    }

    @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
        return null;
    }

    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {

    }

    @Override public void onContextInitialized(IgniteSpiContext spiCtx) throws IgniteSpiException {

    }

    @Override public void onContextDestroyed() {

    }

    @Override public void spiStop() throws IgniteSpiException {

    }

    @Override public void onClientDisconnected(IgniteFuture<?> reconnectFut) {

    }

    @Override public void onClientReconnected(boolean clusterRestarted) {

    }
}
