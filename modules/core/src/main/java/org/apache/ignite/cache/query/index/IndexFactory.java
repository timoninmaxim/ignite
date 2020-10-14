package org.apache.ignite.cache.query.index;

/**
 * Base interface for Ignite index factories.
 */
public interface IndexFactory {
    /**
     * Creates index.
     */
    public Index createIndex(IndexDefinition definition);
}
