package org.apache.ignite.cache.query.index.inline.io;

import org.apache.ignite.cache.query.index.inline.InlineIndexKey;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;

/**
 * This class represents a row in {@link org.apache.ignite.cache.query.index.sorted.SortedIndex}.
 */
// TODO: on delete / update. Cache it in CacheRow.
public class InlinedIndexRow extends InlinedIndexSearchRow {
    /** Object that contains info about original IgniteCache row. */
    private final CacheDataRow cacheRow;

//    /** Cache for original IgniteCache key. */
//    private final Object cacheKey;
//
//    /** Cache for original IgniteCache value. */
//    private final Object cacheVal;
//
//    /** Cache values for indexed keys. */
//    private final Object[] idxKeys;

    /** Constructor. */
    public InlinedIndexRow(CacheDataRow row) {
        this(null, null, row);
    }

        /** Constructor. */
    public InlinedIndexRow(Object[] idxKeys, InlineIndexKey[] schema, CacheDataRow row) {
        super(idxKeys, schema, row.link());

        cacheRow = row;
//        idxKeys = new Object[this.def.getIdxFuncs().size()];
//
//        CacheObjectContext ctx = def.getContext().cacheObjectContext();
//
//        cacheKey = cacheRow.key().value(ctx, false);
//        cacheVal = cacheRow.value().value(ctx, false);
    }

    /**
     * Get indexed value.
     */

    public CacheObject value() {
        return cacheRow.value();
    }

//
//    /** {@inheritDoc} */
//    @Override public int hash() {
//        return cacheRow.hash();
//    }
//
//    /** {@inheritDoc} */
//    @Override public int cacheId() {
//        return cacheRow.cacheId();
//    }
//
//    // TODO: MVCC
//
//    /** {@inheritDoc} */
//    @Override public long mvccCoordinatorVersion() {
//        return 0;
//    }
//
//    /** {@inheritDoc} */
//    @Override public long mvccCounter() {
//        return 0;
//    }
//
//    /** {@inheritDoc} */
//    @Override public int mvccOperationCounter() {
//        return 0;
//    }
//
//    /** {@inheritDoc} */
//    @Override public byte mvccTxState() {
//        return 0;
//    }
//
//
//    @Override public GridCacheVersion version() {
//        return cacheRow.version();
//    }
//
//    @Override public long expireTime() {
//        return cacheRow.expireTime();
//    }
//
//    @Override public int partition() {
//        return cacheRow.partition();
//    }
//
//    @Override public int size() throws IgniteCheckedException {
//        return cacheRow.size();
//    }
//
//    @Override public int headerSize() {
//        return cacheRow.headerSize();
//    }
//
//    @Override public void link(long link) {
//
//    }
//
//    @Override public void key(KeyCacheObject key) {
//
//    }
//
//    @Override public long newMvccCoordinatorVersion() {
//        return 0;
//    }
//
//    @Override public long newMvccCounter() {
//        return 0;
//    }
//
//    @Override public int newMvccOperationCounter() {
//        return 0;
//    }
//
//    @Override public byte newMvccTxState() {
//        return 0;
//    }
}
