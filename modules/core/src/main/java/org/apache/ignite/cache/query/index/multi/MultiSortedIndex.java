package org.apache.ignite.cache.query.index.multi;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.cache.query.index.sorted.Condition;
import org.apache.ignite.internal.util.lang.GridCursor;

/**
 * Interface for sorted Ignite indexes.
 * TODO: Condition array must be synchronized with definition of index functions
 */
public interface MultiSortedIndex<V> extends Index {
    /**
     * Find by equality (select * from t where val = 1 and k = 4).
     * Runs BPlusTree.findOne.
     */
    public V get(Object[] key) throws IgniteCheckedException;

    /**
     * Find by range (select * from t where val between 1 and 4 and k between 4 and 6).
     * Runs BPlusTree.find.
     */
    public GridCursor<V> find(Condition[] conditions) throws IgniteCheckedException;
}


