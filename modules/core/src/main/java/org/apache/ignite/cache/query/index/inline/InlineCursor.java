package org.apache.ignite.cache.query.index.inline;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.index.inline.io.InlinedIndexRow;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.lang.GridCursor;

public class InlineCursor<T> implements GridCursor<T> {

    private final GridCursor<InlinedIndexRow> delegate;
    private final GridCacheContext ctx;

    public InlineCursor(GridCursor<InlinedIndexRow> delegate, GridCacheContext ctx) {
        this.delegate = delegate;
        this.ctx = ctx;
    }

    @Override public boolean next() throws IgniteCheckedException {
        return delegate.next();
    }

    @Override public T get() throws IgniteCheckedException {
        return map(delegate.get());
    }

    private T map(InlinedIndexRow row) {
        return row.value().value(ctx.cacheObjectContext(), false);
    }
}
