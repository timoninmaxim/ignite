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

package org.apache.ignite.internal.processors.query.h2.index;

import java.util.Comparator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.value.CompareMode;
import org.h2.value.Value;

import static org.h2.value.CompareMode.OFF;

/**
 * H2 tree index implementation.
 */
public class H2TreeImpl extends BPlusTree<H2Row, H2Row> {
    /** Cache context. */
    private final GridCacheContext cctx;

    /** Actual columns that current index is consist from. */
    private final IndexColumn[] cols;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param treeName Name of the index.
     * @param input_cols Original indexed columns.
     * @throws IgniteCheckedException If failed.
     */
    public H2TreeImpl(
        GridCacheContext cctx,
        List<IndexColumn> input_cols,
        String treeName,
        RootPage idxSegmentRootPage
    ) throws IgniteCheckedException {
        super(
            treeName,
            cctx.groupId(),
            cctx.group().name(),
            cctx.dataRegion().pageMemory(),
            cctx.shared().wal(),
            cctx.offheap().globalRemoveId(),
            idxSegmentRootPage.pageId().pageId(),
            cctx.offheap().reuseListForIndex(treeName),
            cctx.kernalContext().failure(),
            null
        );

        this.cctx = cctx;

        cols = input_cols.toArray(H2Utils.EMPTY_COLUMNS);

        setIos(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);

        initTree(idxSegmentRootPage.isAllocated());
    }


    /** {@inheritDoc} */
    @Override public H2Row getRow(BPlusIO<H2Row> io, long pageAddr, int idx, Object ignore)
        throws IgniteCheckedException {
        return io.getLookupRow(this, pageAddr, idx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override protected int compare(BPlusIO<H2Row> io, long pageAddr, int idx,
        H2Row row) throws IgniteCheckedException {
        try {
            return compareRows(getRow(io, pageAddr, idx), row);
        }
        catch (DbException ex) {
            throw new IgniteCheckedException("Rows cannot be compared", ex);
        }
    }

    /**
     * Perform sort order correction.
     *
     * @param c Compare result.
     * @param sortType Sort type.
     * @return Fixed compare result.
     */
    private static int fixSort(int c, int sortType) {
        return sortType == SortOrder.ASCENDING ? c : -c;
    }

    /**
     * Compares two H2 rows.
     *
     * @param r1 Row 1.
     * @param r2 Row 2.
     * @return Compare result: see {@link Comparator#compare(Object, Object)} for values.
     */
    public int compareRows(H2Row r1, H2Row r2) {
        assert r2.indexSearchRow() : r2;
        if (r1 == r2)
            return 0;

        for (int i = 0, len = cols.length; i < len; i++) {
            IndexColumn idxCol = cols[i];

            int idx = idxCol.column.getColumnId();

            Value v1 = r1.getValue(idx);
            Value v2 = r2.getValue(idx);

            if (v1 == null || v2 == null)
                continue;

            int c = compareValues(v1, v2);

            if (c != 0)
                return fixSort(c, idxCol.sortType);
        }

        return 0;
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return Comparison result.
     */
    public int compareValues(Value v1, Value v2) {
        return v1 == v2 ? 0 : compareTypeSafe(v1, v2);
    }

    private int compareTypeSafe(Value a, Value b) {
        if (a == b)
            return 0;

        int dataType = Value.getHigherOrder(a.getType(), b.getType());
        a = a.convertTo(dataType);
        b = b.convertTo(dataType);
        // TODO:
        return a.compareTypeSafe(b, CompareMode.getInstance(OFF, 0));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2TreeImpl.class, this, "super", super.toString());
    }

    /**
     * Construct the exception and invoke failure processor.
     *
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Group id.
     * @param pageIds Pages ids.
     * @return New CorruptedTreeException instance.
     */
    @Override protected CorruptedTreeException corruptedTreeException(String msg, Throwable cause, int grpId, long... pageIds) {
        CorruptedTreeException e = new CorruptedTreeException(
            msg, cause, grpId, grpName, cctx.name(), getName(), pageIds);

        processFailure(FailureType.CRITICAL_ERROR, e);

        return e;
    }

    /** {@inheritDoc} */
    @Override protected void temporaryReleaseLock() {
        cctx.kernalContext().cache().context().database().checkpointReadUnlock();
        cctx.kernalContext().cache().context().database().checkpointReadLock();
    }

    /** {@inheritDoc} */
    @Override protected long maxLockHoldTime() {
        long sysWorkerBlockedTimeout = cctx.kernalContext().workersRegistry().getSystemWorkerBlockedTimeout();

        // Using timeout value reduced by 10 times to increase possibility of lock releasing before timeout.
        return sysWorkerBlockedTimeout == 0 ? Long.MAX_VALUE : (sysWorkerBlockedTimeout / 10);
    }
}
