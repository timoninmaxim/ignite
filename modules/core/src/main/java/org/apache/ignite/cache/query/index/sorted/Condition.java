package org.apache.ignite.cache.query.index.sorted;

/**
 * Condition for SortedIndex.find operation
 */
public class Condition<T> {
    /** */
    private final T lower;

    /** */
    private final T upper;

    /** */
    public Condition(T lower, T upper) {
        this.lower = lower;
        this.upper = upper;
    }

    /** */
    public T getLower() {
        return lower;
    }

    /** */
    public T getUpper() {
        return upper;
    }
}
