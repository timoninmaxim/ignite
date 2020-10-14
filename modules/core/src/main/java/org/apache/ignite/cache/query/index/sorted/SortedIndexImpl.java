package org.apache.ignite.cache.query.index.sorted;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.jetbrains.annotations.Nullable;

/**
 * Sorted index implementation.
 * @param <CK> Cache Key.
 * @param <CV> Cache Value.
 * @param <IK> Index Key.
 */
public class SortedIndexImpl<CK, CV, IK> implements SortedIndex<IK, CV> {
    /** Unique ID. */
    private final UUID id = UUID.randomUUID();

    /** Segments. */
    private final IndexTree[] segments;

    /** Index function. */
    private final SortedIndexDefinition def;

    /** Constructor. */
    public SortedIndexImpl(SortedIndexDefinition def, IndexTree[] segments) {
        this.segments = segments.clone();
        this.def = def;
    }

    /** {@inheritDoc} */
    @Override public CV get(IK key) throws IgniteCheckedException {
        GridCacheContext<CK, CV> ctx = (GridCacheContext<CK, CV>) def.getContext();

        KeyCacheObject keyObj = new UserKeyCacheObjectImpl(key, 0);

        // TODO: updateRow set partition to 0
        CacheDataRowAdapter row = new DataRow(keyObj, null, null, 0, -1, ctx.cacheId());

        CacheDataRow result = segment(row).findOne(row);

        if (result == null)
            return null;

        return result.value().value(ctx.cacheObjectContext(), false);
    }

    /** {@inheritDoc} */
    @Override public Cursor<CV> find(IK lower, IK upper) throws IgniteCheckedException {
        GridCacheContext<CK, CV> ctx = (GridCacheContext<CK, CV>) def.getContext();

        CacheSearchRow lowerRow = null;
        CacheSearchRow upperRow = null;

        if (lower != null) {
            // TODO: updateRow set partition to 0
            KeyCacheObject lowerKeyObj = new UserKeyCacheObjectImpl(lower, 0);
            lowerRow = new DataRow(lowerKeyObj, null, null, 0, -1, ctx.cacheId());
        }

        if (upper != null) {
            // TODO: updateRow set partition to 0
            KeyCacheObject upperKeyObj = new UserKeyCacheObjectImpl(upper, 0);
            upperRow = new DataRow(upperKeyObj, null, null, 0, -1, ctx.cacheId());
        }

        // TODO: different partitions?
        GridCursor<CacheDataRow> cursor = segment(lowerRow).find(lowerRow, upperRow);

        return new Cursor<>(cursor, ctx);
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return def.getIdxName();
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(@Nullable CacheDataRow oldRow, @Nullable CacheDataRow newRow) throws IgniteCheckedException {
        assert !(oldRow == null && newRow == null);

        // Delete or clean before Update.
        if (oldRow != null)
            // OldRow is built from link, so it doesn't contain info about index key. So need rehashing.
            remove(rehashRow(oldRow));

        if (newRow == null)
            return;

        // Create.
        try {
            CacheDataRow idxRow = rehashRow(newRow);

            segment(newRow).put(idxRow);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * TODO
     */
    private void remove(CacheDataRow row) throws IgniteCheckedException {
        segment(row).remove(row);
    }

    private CacheDataRow rehashRow(CacheDataRow row) {
        KeyCacheObject keyObj = new UserKeyCacheObjectImpl(indexFunction(row), 0);

        // Overwrite key, to calculate different hash value to store in the tree
        CacheDataRow rehashRow = new DataRow(
            keyObj, row.value(), row.version(), row.partition(), row.expireTime(), row.cacheId());

        rehashRow.link(row.link());

        return rehashRow;
    }

    /**
     * Find Index Key and apply Index Hash Function.
     */
    private IK indexFunction(CacheDataRow row) {
        CacheObjectContext ctx = def.getContext().cacheObjectContext();

        CK key = row.key().value(ctx, false);
        CV val = row.value().value(ctx, false);

        return (IK) def.getIdxFuncs().get(0).apply(key, val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Index> T unwrap(Class<T> clazz) {
        if (clazz == null)
            return null;

        if (clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException(
            String.format("Cannot unwrap [%s] to [%s]", getClass().getName(), clazz.getName())
        );
    }

    /** TODO */
    private IndexTree segment(CacheSearchRow row) {
        return segments[0];
    }
}
