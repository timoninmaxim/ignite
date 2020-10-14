package org.apache.ignite.cache.query.index.sorted;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCursor;

public class Cursor<T> implements GridCursor<T> {

    private final GridCursor<CacheDataRow> delegate;
    private final GridCacheContext ctx;

    public Cursor(GridCursor<CacheDataRow> delegate, GridCacheContext ctx) {
        this.delegate = delegate;
        this.ctx = ctx;
    }

    @Override public boolean next() throws IgniteCheckedException {
        return delegate.next();
    }

    @Override public T get() throws IgniteCheckedException {
        return map(delegate.get());
    }

    private T map(CacheDataRow row) {
        return row.value().value(ctx.cacheObjectContext(), false);
    }
}
