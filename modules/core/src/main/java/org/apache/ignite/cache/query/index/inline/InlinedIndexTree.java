package org.apache.ignite.cache.query.index.inline;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.index.inline.io.InlinedIndexSearchRow;
import org.apache.ignite.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.cache.query.index.inline.io.InlinedIndexRow;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;


/**
 * BPlusTree where nodes stores inlined index keys.
 */
public class InlinedIndexTree extends BPlusTree<InlinedIndexSearchRow, InlinedIndexRow> {

    private final InlineIndexKeyRegistry registry = new InlineIndexKeyRegistry();

    private final int idxKeysSize;

    private final InlineIndexKey[] keys;

    private final CacheGroupContext grp;

    /**
     * Constructor.
     */
    public InlinedIndexTree(
        SortedIndexDefinition def,
        CacheGroupContext grp, String name,
        ReuseList reuseList,
        CacheDataRowStore rowStore, long metaPageId, boolean initNew,
        PageLockListener lockLsnr) throws IgniteCheckedException {
        super(
            name,
            grp.groupId(),
            grp.name(),
            grp.dataRegion().pageMemory(),
            grp.dataRegion().config().isPersistenceEnabled() ? grp.shared().wal() : null,
            grp.offheap().globalRemoveId(),
            metaPageId,
            reuseList,
            grp.shared().kernalContext().failure(),
            lockLsnr
        );

        keys = new InlineIndexKey[def.getIdxFuncs().size()];

        int size = 0;
        for (int i = 0; i < def.getIdxFuncs().size(); ++i) {
            Class<?> idxKeyCls = def.getIdxFuncs().get(i).getIdxKeyCls();
            keys[i] = registry.get(idxKeyCls);
            size += keys[i].size();
        }

        setIos(
            // -1 is required as payload starts with 1, and indexes in list of IOs are with 0.
            (IOVersions<BPlusInnerIO<InlinedIndexSearchRow>>) PageIO.getInnerVersions(size - 1, false),
            (IOVersions<BPlusLeafIO<InlinedIndexSearchRow>>)PageIO.getLeafVersions(size - 1, false));

        initTree(true, size);

        idxKeysSize = size;

        this.grp = grp;
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<InlinedIndexSearchRow> io, long pageAddr, int idx,
        InlinedIndexSearchRow row) throws IgniteCheckedException {

        int fieldOff = 0;

        for (int i = 0; i < keys.length; i++) {
            try {
                int maxSize = idxKeysSize - fieldOff;

                int off = io.offset(idx);

                Comparable currKey = (Comparable) keys[i].get(pageAddr, off + fieldOff, maxSize);

                fieldOff += keys[i].size();

                int cmp = currKey.compareTo(row.idxKeys()[i]);

                if (cmp != 0)
                    return cmp;

            } catch (Exception e) {
                throw new IgniteException("Failed to store new index row.", e);
            }
        }

        return 0;
    }

    /** {@inheritDoc} */
    @Override public InlinedIndexRow getRow(BPlusIO<InlinedIndexSearchRow> io, long pageAddr, int idx, Object ignore)
        throws IgniteCheckedException {

        int off = io.offset(idx);


        // TODO Why idxKeysSize = 5? In Put it is 6.
        long link = PageUtils.getLong(pageAddr, off + idxKeysSize);

        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        // TODO: get from row cache
        row.initFromLink(
            grp,
            CacheDataRowAdapter.RowData.FULL,
            true
        );

        return new InlinedIndexRow(null, null, row);
    }
}
