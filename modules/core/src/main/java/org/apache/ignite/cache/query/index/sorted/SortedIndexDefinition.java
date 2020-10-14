package org.apache.ignite.cache.query.index.sorted;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.index.IndexDefinition;
import org.apache.ignite.cache.query.index.inline.InlineIndexKey;
import org.apache.ignite.cache.query.index.inline.InlineIndexKeyRegistry;
import org.apache.ignite.internal.processors.cache.GridCacheContext;

/**
 *
 */
// TODO: SortedIndexDefinition as index function is fixed generic
public class SortedIndexDefinition implements IndexDefinition {
    /** Cache context index is created for. */
    private GridCacheContext ctx;

    /** Optional user defined function that provides calculation of index key hash code. */
    private final List<SortedIndexFunction> idxFuncs;

    private final InlineIndexKey[] schema;

    /** Unique index name. */
    private final String idxName;

    // TODO?
    /** Segments. */
    private final int segments;

    /** Constructor. */
    public SortedIndexDefinition(GridCacheContext ctx, String idxName, int segments, SortedIndexFunction... idxFuncs) {
        if (idxFuncs.length == 0)
            throw new IgniteException("At least one Index function must be specified");

        this.ctx = ctx;
        this.idxName = idxName;
        this.segments = segments;

        this.idxFuncs = new ArrayList<>();
        this.idxFuncs.addAll(Arrays.asList(idxFuncs));

        InlineIndexKeyRegistry registry = new InlineIndexKeyRegistry();

        schema = new InlineIndexKey[idxFuncs.length];
        for (int i = 0; i < idxFuncs.length; ++i)
            schema[i] = registry.get(idxFuncs[i].getIdxKeyCls());
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext getContext() {
        return ctx;
    }

    public String getIdxName() {
        return idxName;
    }

    public int getSegments() {
        return segments;
    }

    public List<SortedIndexFunction> getIdxFuncs() {
        return idxFuncs;
    }

    public InlineIndexKey[] getSchema() {
        return schema;
    }
}
