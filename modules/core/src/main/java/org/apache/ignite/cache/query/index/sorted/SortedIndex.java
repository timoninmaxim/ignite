package org.apache.ignite.cache.query.index.sorted;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.internal.util.lang.GridCursor;

/**
 * Interface for sorted Ignite indexes.
 */
public interface SortedIndex<K, V> extends Index {
    /**
     * Find by equality (select * from t where val = 1).
     * Runs BPlusTree.findOne.
     */
    public V get(K key) throws IgniteCheckedException;

    /**
     * Find by range (select * from t where t between 1 and 4).
     * Runs BPlusTree.find.
     */
    public GridCursor<V> find(K lower, K upper) throws IgniteCheckedException;
}
