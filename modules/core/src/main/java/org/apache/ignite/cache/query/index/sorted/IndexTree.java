package org.apache.ignite.cache.query.index.sorted;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;

/**
 * IndexTree overrides {@link #compare} due to CacheDataTree goes directly to link to compare keys.
 * But indexes key is calculated with {@link SortedIndexFunction} and isn't stored within cache row.
 */
public class IndexTree extends CacheDataTree {

    private final CacheObjectContext coctx;

    private final CacheGroupContext cgctx;

    private final SortedIndexDefinition def;

    /**
     * @param grp        Cache group.
     * @param name       Tree name.
     * @param reuseList  Reuse list.
     * @param rowStore   Row store.
     * @param metaPageId Meta page ID.
     * @param initNew    Initialize new index.
     * @param lockLsnr
     * @throws IgniteCheckedException If failed.
     */
    public IndexTree(
        SortedIndexDefinition def,
        CacheGroupContext grp, String name,
        ReuseList reuseList,
        CacheDataRowStore rowStore, long metaPageId, boolean initNew,
        PageLockListener lockLsnr) throws IgniteCheckedException {
        super(grp, name, reuseList, rowStore, metaPageId, initNew, lockLsnr);

        this.def = def;
        cgctx = grp;
        coctx = grp.cacheObjectContext();
    }

    /** {@inheritDoc} */
    @Override public int compare(BPlusIO<CacheSearchRow> iox, long pageAddr, int idx, CacheSearchRow row) {
        return applySortOrder(compareNoOrder(iox, pageAddr, idx, row), def.getIdxFuncs().get(0).getSortOrder());
    }

    /** */
    public int compareNoOrder(BPlusIO<CacheSearchRow> iox, long pageAddr, int idx, CacheSearchRow row) {
        try {
            long currLink = getLink(pageAddr, iox.offset(idx));

            CacheDataRowAdapter currRow = new CacheDataRowAdapter(currLink);

            currRow.initFromLink(
                cgctx,
                CacheDataRowAdapter.RowData.FULL,
                true
            );

            Object currCacheKey = currRow.key().value(coctx, false);
            Object currCacheVal = currRow.value().value(coctx,false);

            // Compare hashes:
            // - row hash calculates from user request.
            // - current row hash calculates from indexed columns while storing the row.
            // If hashes are equal then compare cache keys.
            // TODO: prepare caches only if inserting! If just search and hash are equals than fast return to 0.
            if (row.hash() == ((RowLinkIO)iox).getHash(pageAddr, idx))
                return compareCacheKeys(currCacheKey, row);

            Comparable currIdxKey = (Comparable) def.getIdxFuncs().get(0).apply(currCacheKey, currCacheVal);
            Comparable rowIdxKey = row.key().value(coctx, false);

            // Compare current read value with input param CacheSearchRow
            int cmp = currIdxKey.compareTo(rowIdxKey);

            if (cmp != 0)
                return cmp;

            // Compare primary cache keys. To enable duplicates (non-unique index).
            return compareCacheKeys(currCacheKey, row);

        } catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to compare keys.", e);
        }
    }

    private long getLink(long pageAddr, int offset) {
        return PageUtils.getLong(pageAddr, offset);
    }

    // TODO: Unique index implementation should skip this check. But how to revert cache put operation?
    private int compareCacheKeys(Object currCacheKey, CacheSearchRow row) throws IgniteCheckedException {
        // Insert of new row to index.
        if (row.link() != 0) {
            CacheDataRowAdapter newRow = new CacheDataRowAdapter(row.link());

            newRow.initFromLink(
                cgctx,
                CacheDataRowAdapter.RowData.KEY_ONLY,
                true
            );

            Object newCacheKey = newRow.key().value(coctx, false);

            return ((Comparable) currCacheKey).compareTo(newCacheKey);
        }

        // Find matching row in index.
        return 0;
    }

    /**
     * Perform sort order correction.
     */
    private int applySortOrder(int c, SortOrder order) {
        return order == SortOrder.ASC ? c : -c;
    }
}
