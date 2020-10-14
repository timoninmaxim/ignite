package org.apache.ignite.cache.query.index.multi;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.cache.query.index.sorted.SortedIndexFunction;
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
public class MultiIndexTree extends CacheDataTree {

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
    public MultiIndexTree(
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
        try {
            // Compare hashes (to avoid unmarshalling data):
            // row hash calculates from user request.
            // current row hash calculates from indexed columns while storing the row.
            if (row.hash() == ((RowLinkIO)iox).getHash(pageAddr, idx))
                return 0;

            long link = getLink(pageAddr, iox.offset(idx));

            CacheDataRowAdapter currRow = new CacheDataRowAdapter(link);

            currRow.initFromLink(
                cgctx,
                CacheDataRowAdapter.RowData.FULL,
                true
            );

            Object currKey = currRow.key().value(coctx, false);
            Object currVal = currRow.value().value(coctx,false);

            Object[] rowIdxKeys = row.key().value(coctx, false);

            // Think that order of index functions and conditions are the same.
            // Number of conditions could be less than index functions.
            for (int i = 0; i < rowIdxKeys.length; ++i) {
                Comparable currIdxKey = (Comparable) def.getIdxFuncs().get(i).apply(currKey, currVal);

                int cmp = ((Comparable) rowIdxKeys[i]).compareTo(currIdxKey);

                if (cmp != 0)
                    return cmp;
            }

            return 0;

        } catch (IgniteCheckedException e) {

            throw new IgniteException("Failed to compare keys.", e);
        }
    }

    private long getLink(long pageAddr, int offset) {
        return PageUtils.getLong(pageAddr, offset);
    }
}
