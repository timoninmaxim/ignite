/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cache.query.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.internal.cache.query.IndexCondition;
import org.apache.ignite.internal.cache.query.RangeIndexCondition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowPartialImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexSearchRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IndexQueryDesc;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Processor of {@link IndexQuery}.
 */
public class IndexQueryProcessor {
    /** */
    private final IndexProcessor idxProc;

    /** */
    private final Map<String, ValClassProperties> valClsProps = new ConcurrentHashMap<>();

    /** */
    public IndexQueryProcessor(IndexProcessor idxProc) {
        this.idxProc = idxProc;
    }

    /** Run query on local node. */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocal(
        GridCacheContext<K, V> cctx, IndexQueryDesc idxQryDesc, IgniteBiPredicate<K, V> predicate,
        IndexQueryContext qryCtx, boolean keepBinary) throws IgniteCheckedException {

        Class<?> valCls = idxQryDesc.valCls() != null ? loadValClass(cctx, idxQryDesc.valCls()) : null;

        Index idx = index(cctx, valCls, idxQryDesc, predicate);

        if (idx == null) {
            throw new IgniteCheckedException(
                "No index matches index query. " +
                    "Cache=" + cctx.name() + "; Qry=" + idxQryDesc + "; predicate=" + (predicate != null));
        }

        GridCursor<IndexRow> cursor = query(cctx, idx, idxQryDesc.idxCond(), predicate, qryCtx);

        IndexDefinition idxDef = idxProc.indexDefinition(idx.id());

        if (predicate != null)
            prepareValClsProps(cctx, idxDef, valCls);

        // Map IndexRow to Cache Key-Value pair.
        return new GridCloseableIteratorAdapter<IgniteBiTuple<K, V>>() {
            private IndexRow currVal;

            private final CacheObjectContext coctx = cctx.cacheObjectContext();

            /** {@inheritDoc} */
            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (currVal != null)
                    return true;

                while (cursor.next()) {
                    currVal = cursor.get();

                    if (predicate == null)
                        return true;

                    if (checkPredicate(idxQryDesc.valCls(), predicate, currVal))
                        return true;
                }

                currVal = null;

                return false;
            }

            /** {@inheritDoc} */
            @Override protected IgniteBiTuple<K, V> onNext() {
                if (currVal == null)
                    if (!hasNext())
                        throw new NoSuchElementException();

                IndexRow row = currVal;

                currVal = null;

                K k = (K) CacheObjectUtils.unwrapBinaryIfNeeded(coctx, row.cacheDataRow().key(), keepBinary, false);
                V v = (V) CacheObjectUtils.unwrapBinaryIfNeeded(coctx, row.cacheDataRow().value(), keepBinary, false);

                return new IgniteBiTuple<>(k, v);
            }

            /** */
            private boolean checkPredicate(String valCls, IgniteBiPredicate<K, V> predicate, IndexRow row) {
                ValClassProperties props = valClsProps.get(valCls);

                try {
                    V val = props.value(idxDef, row);

                    // TODO: handle NPE and fallback to unbinary? Or make proxy? Key can apply too, as it inlined.
                    return predicate.apply(null, val);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                return false;
            }
        };
    }

    /** Get index to run query by specified description. */
    private Index index(GridCacheContext cctx, Class<?> valCls, IndexQueryDesc idxQryDesc, IgniteBiPredicate predicate) {
        String tableName = cctx.kernalContext().query().tableName(cctx.name(), valCls);

        if (tableName == null)
            return null;

        if (predicate != null && idxQryDesc.idxName() == null && idxQryDesc.idxCond() == null)
            return null;

        // Find index by specified name.
        if (idxQryDesc.idxName() != null) {
            String name = "_key_PK".equals(idxQryDesc.idxName()) ? "_key_PK" : idxQryDesc.idxName().toUpperCase();

            String schema = idxQryDesc.schema() == null ? cctx.name() : idxQryDesc.schema();

            IndexName idxName = new IndexName(cctx.name(), schema, tableName, name);

            Index idx = idxProc.index(idxName);

            if (idx == null)
                return null;

            return checkIndex(idxProc.indexDefinition(idx.id()), idxQryDesc.idxCond()) ? idx : null;
        }

        // Try get index by list of fields to query.
        // Check all indexes by cache to find best index match: count of index fields equals to count of index condition fields.
        Collection<Index> idxs = idxProc.indexes(cctx);

        Index idx = null;
        int idxFieldsCnt = 0;

        for (Index i: idxs) {
            IndexDefinition idxDef = idxProc.indexDefinition(i.id());

            if (!tableName.equals(idxDef.idxName().tableName()))
                continue;

            int fldsCnt = idxDef.indexKeyDefinitions().size();

            if (checkIndex(idxDef, idxQryDesc.idxCond())) {
                if (idx == null) {
                    idx = i;
                    idxFieldsCnt = fldsCnt;
                }
                else if (fldsCnt < idxFieldsCnt) {
                    idx = i;
                    idxFieldsCnt = fldsCnt;
                }
                else continue;

                // Best match. Query condition matches full index.
                if (idxQryDesc.idxCond().fields().size() == idxDef.indexKeyDefinitions().size())
                    break;
            }
        }

        return idx;
    }

    /** Checks that specified index matches index query condition. */
    private boolean checkIndex(IndexDefinition idxDef, IndexCondition idxCond) {
        if (idxCond.fields().size() > idxDef.indexKeyDefinitions().size())
            return false;

        for (int i = 0; i < idxCond.fields().size(); i++) {
            if (!idxDef.indexKeyDefinitions().get(i).name().equalsIgnoreCase(idxCond.fields().get(i)))
                return false;
        }

        return true;
    }

    /** */
    private Class<?> loadValClass(GridCacheContext cctx, String valClsName) throws IgniteCheckedException {
        try {
            ClassLoader clsLdr = U.resolveClassLoader(cctx.kernalContext().config());

            return clsLdr.loadClass(valClsName);
        }
        catch (ClassNotFoundException e) {
            throw new IgniteCheckedException("No cache serves class: " + valClsName);
        }
    }

    /** */
    private void prepareValClsProps(GridCacheContext cctx, IndexDefinition def, Class<?> valCls) {
        valClsProps.computeIfAbsent(valCls.getName(), (cls) -> {
            GridQueryTypeDescriptor desc = cctx.kernalContext().query().typeDescriptor(cctx.name(), valCls.getSimpleName());

            try {
                return new ValClassProperties(valCls, def, cctx.cacheObjectContext(), (QueryTypeDescriptorImpl) desc);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();

                return null;
            }
        });
    }

    /** Runs a query and return single cursor or cursor over multiple index segments. */
    private GridCursor<IndexRow> query(GridCacheContext cctx, Index idx, IndexCondition idxCond, IgniteBiPredicate predicate, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        int segmentsCnt = cctx.isPartitioned() ? cctx.config().getQueryParallelism() : 1;

        if (segmentsCnt == 1)
            return query(0, idx, idxCond, predicate, qryCtx);

        final GridCursor<IndexRow>[] segments = new GridCursor[segmentsCnt];

        // Actually it just traverse BPlusTree to find boundaries. It's too fast to parallelize this.
        for (int i = 0; i < segmentsCnt; i++)
            segments[i] = query(i, idx, idxCond, predicate, qryCtx);

        return new SegmentedIndexCursor(segments, ((SortedIndexDefinition) idxProc.indexDefinition(idx.id())).rowComparator());
    }

    /** Coordinate query conditions. */
    private GridCursor<IndexRow> query(int segment, Index idx, IndexCondition idxCond, IgniteBiPredicate predicate, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        if (idxCond instanceof RangeIndexCondition)
            return treeIndexRange((InlineIndex) idx, (RangeIndexCondition) idxCond, segment, predicate, qryCtx);

        throw new IllegalStateException("Doesn't support index condition: " + idxCond.getClass().getName());
    }

    /** Runs range query over specified segment. */
    private GridCursor<IndexRow> treeIndexRange(InlineIndex idx, RangeIndexCondition cond, int segment,
        IgniteBiPredicate predicate, IndexQueryContext qryCtx) throws IgniteCheckedException {

        InlineIndexRowHandler hnd = idx.segment(0).rowHandler();

        IndexKey[] lowerBounds = cond.lowers() == null ? null : new IndexKey[hnd.indexKeyDefinitions().size()];
        IndexKey[] upperBounds = cond.uppers() == null ? null : new IndexKey[hnd.indexKeyDefinitions().size()];

        IndexRow lower = cond.lowers() == null ? null : new IndexSearchRowImpl(lowerBounds, hnd);
        IndexRow upper = cond.uppers() == null ? null : new IndexSearchRowImpl(upperBounds, hnd);

        for (int i = 0; i < cond.fields().size(); i++) {
            String f = cond.fields().get(i);

            IndexKeyDefinition def = hnd.indexKeyDefinitions().get(i);

            if (!def.name().equalsIgnoreCase(f))
                throw new IgniteCheckedException("Range query doesn't match index '" + idx.name() + "'");

            if (lowerBounds != null) {
                Object val = cond.lowers().get(i);

                if (val instanceof IndexQuery.Null)
                    val = null;

                IndexKey l = IndexKeyFactory.wrap(
                    val, def.idxType(), idx.segment(0).cacheContext().cacheObjectContext(), hnd.indexKeyTypeSettings());

                lowerBounds[i] = l;
            }

            if (upperBounds != null) {
                Object val = cond.uppers().get(i);

                if (val instanceof IndexQuery.Null)
                    val = null;

                IndexKey u = IndexKeyFactory.wrap(
                    val, def.idxType(), idx.segment(0).cacheContext().cacheObjectContext(), hnd.indexKeyTypeSettings());

                upperBounds[i] = u;
            }
        }

        GridCursor<IndexRow> findRes = idx.find(lower, upper, segment, qryCtx, predicate != null);

        boolean checkLower = !cond.lowerInclusive() && cond.lowers() != null;
        boolean checkUpper = !cond.upperInclusive() && cond.uppers() != null;

        if (!checkLower && !checkUpper)
            return findRes;

        return new GridCursor<IndexRow>() {
            /** Whether returns first row. */
            private boolean returnFirst;

            private IndexRowComparator rowCmp = ((SortedIndexDefinition) idxProc.indexDefinition(idx.id())).rowComparator();

            /** {@inheritDoc} */
            @Override public boolean next() throws IgniteCheckedException {
                if (!findRes.next())
                    return false;

                if (checkLower && !returnFirst) {
                    while (match(get(), lower, cond.lowers().size())) {
                        if (!findRes.next())
                            return false;
                    }

                    returnFirst = true;
                }

                if (checkUpper && match(get(), upper, cond.uppers().size()))
                    return false;

                return true;
            }

            /** {@inheritDoc} */
            @Override public IndexRow get() throws IgniteCheckedException {
                return findRes.get();
            }

            /** Return {@code true} if specified row fully match specified condition. */
            private boolean match(IndexRow row, IndexRow cond, int condKeysCnt) throws IgniteCheckedException {
                for (int i = 0; i < condKeysCnt; i++) {
                    if (rowCmp.compareKey(row, cond, i) != 0)
                        return false;
                }

                return true;
            }
        };
    }

    /** Single cursor over multiple segments. Next value is choose with the index row comparator. */
    private class SegmentedIndexCursor implements GridCursor<IndexRow> {
        /** Cursors over segments. */
        private final GridCursor<IndexRow>[] cursors;

        /** Whether returns first value for user. */
        private boolean returnFirst;

        /** Offset of current segmented cursor to return value. */
        private int cursorOff;

        /** Comparator to compare index rows. */
        private final Comparator<GridCursor<IndexRow>> cursorComp;

        /** */
        SegmentedIndexCursor(GridCursor<IndexRow>[] cursors, IndexRowComparator rowCmp) {
            this.cursors = cursors;

            cursorComp = new Comparator<GridCursor<IndexRow>>() {
                @Override public int compare(GridCursor<IndexRow> o1, GridCursor<IndexRow> o2) {
                    try {
                        if (o1 == o2)
                            return 0;

                        if (o1 == null)
                            return -1;

                        if (o2 == null)
                            return 1;

                        return rowCmp.compareKey(o1.get(), o2.get(), 0);

                    } catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            };
        }

        /** {@inheritDoc} */
        @Override public boolean next() throws IgniteCheckedException {
            if (!returnFirst) {
                for (int i = 0; i < cursors.length; i++) {
                    if (!cursors[i].next()) {
                        cursors[i] = null;
                        cursorOff++;
                    }
                }

                if (cursorOff == cursors.length)
                    return false;

                Arrays.sort(cursors, cursorComp);

                returnFirst = true;

            } else {
                if (cursorOff == cursors.length)
                    return false;

                if (!cursors[cursorOff].next())
                    cursors[cursorOff++] = null;

                bubbleUp();
            }

            return cursorOff != cursors.length;
        }

        /** {@inheritDoc} */
        @Override public IndexRow get() throws IgniteCheckedException {
            return cursors[cursorOff].get();
        }

        /** */
        private void bubbleUp() {
            for (int i = cursorOff, last = cursors.length - 1; i < last; i++) {
                if (cursorComp.compare(cursors[i], cursors[i + 1]) <= 0)
                    break;

                U.swap(cursors, i, i + 1);
            }
        }
    }

    private static class ValClassProperties {
        /** */
        private final Map<String, GridQueryProperty> props = new HashMap<>();

        private final Class<?> valCls;

        /** */
        public <V> V value(IndexDefinition def, IndexRow row) throws Exception {
            V o = (V) valCls.getConstructor().newInstance();

            IndexRowPartialImpl r = (IndexRowPartialImpl) row;

            for (int i = 0; i < def.indexKeyDefinitions().size(); i++) {
                if (r.key(i) == null)
                    break;

                IndexKeyDefinition d = def.indexKeyDefinitions().get(i);

                if (!props.containsKey(d.name()))
                    continue;

                props.get(d.name()).setValue(null, o, r.key(i).key());
            }

            return o;
        }

        /** */
        public ValClassProperties(Class<?> valCls, IndexDefinition def, CacheObjectContext coctx, QueryTypeDescriptorImpl desc)
            throws IgniteCheckedException {

            this.valCls = valCls;

            // TODO: Make wrapper for property to proxy initialization of CacheDataRow?? Or just fallback to unwrap logic.
            for (IndexKeyDefinition d: def.indexKeyDefinitions()) {
                String fld = d.name();

                // TODO:
                if ("_KEY".equals(fld))
                    continue;

                Class<?> fldCls = desc.fields().get(fld);

                // TODO: need reverted map.
                for (Map.Entry<String, String> e: desc.aliases().entrySet()) {
                    if (fld.equals(e.getValue())) {
                        fld = e.getKey();
                        break;
                    }
                }

                GridQueryProperty prop = classProperty(
                    fld,
                    fldCls.getName(),
                    desc,
                    coctx);

                props.put(d.name(), prop);
            }
        }

        /** */
        private GridQueryProperty classProperty(String field, String fieldType, QueryTypeDescriptorImpl d, CacheObjectContext coCtx)
            throws IgniteCheckedException {
            return QueryUtils.buildProperty(
                d.keyClass(),
                valCls,
                d.keyFieldName(),
                d.valueFieldName(),
                field,
                U.classForName(fieldType, Object.class),
                d.aliases(),
                false, // not null TODO: do not need check it, we need just to create object without validation.
                coCtx);
        }
    }
}
