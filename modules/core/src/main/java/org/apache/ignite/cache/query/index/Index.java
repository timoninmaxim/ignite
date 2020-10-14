package org.apache.ignite.cache.query.index;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.jetbrains.annotations.Nullable;

// TODO: reduce phase?

/** Basic interface for Ignite indexes. */
public interface Index {
    /**
     * Unique ID.
     */
    public UUID id();

    /**
     * Unique name.
     */
    public String name();

    /**
     * Callback that runs when the underlying cache is updated.
     */
    public void onUpdate(@Nullable CacheDataRow oldRow, @Nullable CacheDataRow newRow) throws IgniteCheckedException;

    /**
     * Provides a standard way to access the underlying concrete index
     * implementation to provide access to further, proprietary features.
     */
    public <T extends Index> T unwrap(Class<T> clazz);
}
