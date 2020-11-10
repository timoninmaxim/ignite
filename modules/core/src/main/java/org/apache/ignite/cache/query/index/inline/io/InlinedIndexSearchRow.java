package org.apache.ignite.cache.query.index.inline.io;

import org.apache.ignite.cache.query.index.inline.InlineIndexKey;

/**
 * This class represents a row in {@link org.apache.ignite.cache.query.index.sorted.SortedIndex}.
 */
// TODO: on delete / update. Cache it in CacheRow.
public class InlinedIndexSearchRow {
    private final long link;

    // Row should contain this info, because IO are shared and don't store info about indexes.
    // TODO: by fact this is schema description
    private final InlineIndexKey[] schema;

    private final Object[] idxKeys;

    /**
     * If {@code true} then length of {@link #idxKeys} must be equal to length of {@link #schema}, so use full
     * schema to search. If {@code false} then it's possible to use only part of schema for search. Only first
     */
    private final boolean fullSchemaSearch;

    /** Constructor. */
    public InlinedIndexSearchRow(Object[] idxKeys, InlineIndexKey[] schema, boolean fullSchemaSearch) {
        this(idxKeys, schema, fullSchemaSearch, 0);
    }

    /** Constructor. */
    public InlinedIndexSearchRow(Object[] idxKeys, InlineIndexKey[] schema) {
        this(idxKeys, schema, true, 0);
    }

    /** Constructor. */
    public InlinedIndexSearchRow(Object[] idxKeys, InlineIndexKey[] schema, boolean fullSchemaSearch, long link) {
        this.idxKeys = idxKeys;
        this.schema = schema;
        this.fullSchemaSearch = fullSchemaSearch;
        this.link = link;
    }

    public long link() {
        return link;
    }

    public Object[] idxKeys() {
        return idxKeys;
    }

    public InlineIndexKey[] getSchema() {
        return schema;
    }

    public boolean isFullSchemaSearch() {
        return fullSchemaSearch;
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
