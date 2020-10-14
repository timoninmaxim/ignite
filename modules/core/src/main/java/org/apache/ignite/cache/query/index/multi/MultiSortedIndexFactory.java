package org.apache.ignite.cache.query.index.multi;

import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.cache.query.index.IndexDefinition;
import org.apache.ignite.cache.query.index.IndexFactory;
import org.apache.ignite.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;

/** */
public class MultiSortedIndexFactory implements IndexFactory {
    /** {@inheritDoc} */
    @Override public Index createIndex(IndexDefinition def) {
        SortedIndexDefinition sdef = (SortedIndexDefinition) def;

        MultiIndexTree[] trees = new MultiIndexTree[sdef.getSegments()];

        try {
            for (int i = 0; i < sdef.getSegments(); ++i) {
                // Required for persistence.
                IgniteCacheDatabaseSharedManager db = def.getContext().shared().database();
                db.checkpointReadLock();

                try {
                    RootPage page = getRootPage(def.getContext(), sdef.getIdxName(), i);
                    trees[i] = createIndexSegment(sdef, i, page);

                } finally {
                    db.checkpointReadUnlock();
                }

            }

            return new MultiSortedIndexImpl(sdef, trees);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private MultiIndexTree createIndexSegment(SortedIndexDefinition def, int segment, RootPage rootPage) throws Exception {
        CacheGroupContext gctx = def.getContext().group();

        CacheDataRowStore rowStore = new CacheDataRowStore(gctx, gctx.freeList(), segment);

        return new MultiIndexTree(
            def,
            gctx,
            def.getIdxName(),
            def.getContext().offheap().reuseListForIndex(def.getIdxName()),
            rowStore,
            rootPage.pageId().pageId(),
            rootPage.isAllocated(),
            null);
    }

    /** */
    private RootPage getRootPage(GridCacheContext ctx, String idxName, int segment) throws Exception {
        return ctx.offheap().rootPageForIndex(ctx.cacheId(), idxName, segment);
    }
}
