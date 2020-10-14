package org.apache.ignite.cache.query.index.sorted;

import java.util.function.BiFunction;

/**
 * Index function.
 * @param <CK> Cache Key.
 * @param <CV> Cache Value.
 * @param <IK> Index Key.
 */
public class SortedIndexFunction<CK, CV, IK extends Comparable<IK>> {
    /** Sort order. */
    private final SortOrder sortOrder;

    /** Index function. */
    private final BiFunction<CK, CV, IK> idxFunction;

    /** Index key class. */
    private final Class<IK> idxKeyCls;

    /**
     * @param idxFunction Index function.
     */
    public SortedIndexFunction(BiFunction<CK, CV, IK> idxFunction, Class<IK> idxKeyCls) {
        this(idxFunction, idxKeyCls, SortOrder.ASC);
    }

    /**
     * @param idxFunction Index function.
     * @param sortOrder Sort order.
     */
    public SortedIndexFunction(BiFunction<CK, CV, IK> idxFunction, Class<IK> idxKeyCls, SortOrder sortOrder) {
        this.sortOrder = sortOrder;
        this.idxFunction = idxFunction;
        this.idxKeyCls = idxKeyCls;
    }

    /** */
    public SortOrder getSortOrder() {
        return sortOrder;
    }

    /**
     * Applies this function to the given arguments.
     *
     * @param cacheKey the first function argument
     * @param cacheVal the second function argument
     * @return the function result
     */
    public IK apply(CK cacheKey, CV cacheVal) {
        return idxFunction.apply(cacheKey, cacheVal);
    }

    /** */
    public Class<IK> getIdxKeyCls() {
        return idxKeyCls;
    }
}
