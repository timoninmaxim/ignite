package org.apache.ignite.cache.query.index.inline;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.cache.query.index.inline.io.InlinedIndexRow;
import org.apache.ignite.cache.query.index.inline.io.InlinedIndexSearchRow;
import org.apache.ignite.cache.query.index.multi.MultiSortedIndex;
import org.apache.ignite.cache.query.index.sorted.Condition;
import org.apache.ignite.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cacheobject.UserKeyCacheObjectImpl;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.jetbrains.annotations.Nullable;

/**
 * Sorted index implementation.
 * @param <CK> Cache Key.
 * @param <CV> Cache Value.
 */
public class InlineIndexImpl<CK, CV> implements MultiSortedIndex<CV> {
    /** Unique ID. */
    private final UUID id = UUID.randomUUID();

    /** Segments. */
    private final InlinedIndexTree[] segments;

    /** Index function. */
    private final SortedIndexDefinition def;

    /** Constructor. */
    public InlineIndexImpl(SortedIndexDefinition def, InlinedIndexTree[] segments) {
        this.segments = segments.clone();
        this.def = def;
    }

    /** {@inheritDoc} */
    @Override public CV get(Object[] keys) throws IgniteCheckedException {
        if (keys.length != def.getSchema().length)
            throw new IgniteCheckedException("Number of index keys and search keys are different.");

        GridCacheContext<CK, CV> ctx = (GridCacheContext<CK, CV>) def.getContext();

        // TODO: updateRow set partition to 0
        InlinedIndexSearchRow irow = new InlinedIndexSearchRow(keys, def.getSchema());

        InlinedIndexRow result = segment().findOne(irow);

        if (result == null)
            return null;

        return result.value().value(ctx.cacheObjectContext(), false);
    }

    /** {@inheritDoc} */
    @Override public InlineCursor<CV> find(Condition[] conditions) throws IgniteCheckedException {
        validateConditions(conditions);

        GridCacheContext<CK, CV> ctx = (GridCacheContext<CK, CV>) def.getContext();

        Object[] lowerBounds = new Object[conditions.length];
        Object[] upperBounds = new Object[conditions.length];

        for (int i = 0; i < conditions.length; ++i) {
            Condition c = conditions[i];

            lowerBounds[i] = c.getLower();
            upperBounds[i] = c.getUpper();
        }

        // TODO: different partitions?
        GridCursor<InlinedIndexRow> cursor = segment().find(
            new InlinedIndexSearchRow(lowerBounds, def.getSchema()),
            new InlinedIndexSearchRow(upperBounds, def.getSchema()));

        return new InlineCursor<>(cursor, ctx);
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
        if (oldRow != null) {
            // OldRow is built from link, so it doesn't contain info about index key. So need rehashing.
            // TODO: rehashing?
            Object[] oldKeys = prepareIdxKey(oldRow);

            segment().remove(new InlinedIndexRow(oldKeys, def.getSchema(), oldRow));
        }

        if (newRow == null)
            return;

        // Create.
        try {
            Object[] newKeys = prepareIdxKey(newRow);

            segment().put(new InlinedIndexRow(newKeys, def.getSchema(), newRow));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    private Object[] indexFunction(CacheDataRow row) {
        CacheObjectContext ctx = def.getContext().cacheObjectContext();

        CK key = row.key().value(ctx, false);
        CV val = row.value().value(ctx, false);

        Object[] idxKeys = new Object[def.getIdxFuncs().size()];

        for (int i = 0; i < def.getIdxFuncs().size(); ++i)
            idxKeys[i] = def.getIdxFuncs().get(i).apply(key, val);

        return idxKeys;
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
    // TODO: how to calculate segments?
    private InlinedIndexTree segment() {
        return segments[0];
    }

    /** */
    private void validateConditions(Condition[] conditions) {
        if (conditions.length == def.getIdxFuncs().size())
            return;

        if (conditions.length > def.getIdxFuncs().size())
            // TODO: queries like `select * from t where val < 10 or val > 20` are splitted on 2 index queries?
            throw new IgniteException("Number of conditions differs from index functions.");

        // TODO: less?
    }

    private Object[] prepareIdxKey(CacheDataRow cacheRow) {
        Object cacheKey = cacheRow.key().value(def.getContext().cacheObjectContext(), false);
        Object cacheVal = cacheRow.value().value(def.getContext().cacheObjectContext(), false);

        Object[] keys = new Object[def.getIdxFuncs().size()];

        for (int i = 0; i < def.getIdxFuncs().size(); ++i)
            keys[i] = def.getIdxFuncs().get(i).apply(cacheKey, cacheVal);

        return keys;
    }
}
