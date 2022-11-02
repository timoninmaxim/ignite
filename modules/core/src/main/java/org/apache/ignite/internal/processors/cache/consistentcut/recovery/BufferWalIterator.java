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

package org.apache.ignite.internal.processors.cache.consistentcut.recovery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Iterator wrapper over WALIterator {@link #walIt} that stores read records in buffer to re-read them later.
 *
 * Consistent Cut algorithm requires to re-read some parts of WAL multiple times, then usage of the buffer is cheaper
 * than frequent use of {@link IgniteWriteAheadLogManager#replay(WALPointer)}.
 */
public class BufferWalIterator extends GridIteratorAdapter<IgniteBiTuple<WALPointer, WALRecord>> implements WALIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /** Buffer to store read items from {@link #walIt} to re-read them at future. */
    private List<IgniteBiTuple<WALPointer, WALRecord>> buf;

    /** Iterator over copy of buffer at moment of iterator creation. */
    private Iterator<IgniteBiTuple<WALPointer, WALRecord>> bufIt;

    /** Underlying WAL iterator. */
    private final WALIterator walIt;

    /** Buffer Mode: CLEAN or STORE. */
    private BufferedMode mode;

    /** Current record to read from the buffer. */
    private IgniteBiTuple<WALPointer, WALRecord> curBufItem;

    /** Filters records by transaction version. */
    private IgnitePredicate<GridCacheVersion> txVerFilter;

    /** Last delivered pointer from buffer or WAL. */
    private WALPointer lastReadPtr;

    /** */
    public BufferWalIterator(WALIterator walIt) {
        this.walIt = walIt;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        return bufHasNext() || walIt.hasNext();
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<WALPointer, WALRecord> nextX() throws IgniteCheckedException {
        // Returns item from the buffer.
        if (curBufItem != null) {
            IgniteBiTuple<WALPointer, WALRecord> next = curBufItem;

            lastReadPtr = curBufItem.get1();

            curBufItem = null;

            return next;
        }

        IgniteBiTuple<WALPointer, WALRecord> rec = walIt.next();

        lastReadPtr = rec.get1();

        if (mode == BufferedMode.STORE)
            buf.add(rec);

        return rec;
    }

    /**
     * @return {@code true} if buffer has next item, otherwise {@code false}.
     */
    private boolean bufHasNext() {
        if (bufIt == null)
            return false;

        while (bufIt.hasNext()) {
            curBufItem = bufIt.next();

            if (mode == BufferedMode.CLEAN)
                buf.remove(0);

            if (!walRecordFilter(curBufItem.getValue()))
                continue;

            break;
        }

        if (curBufItem == null) {
            bufIt = null;

            if (mode == BufferedMode.CLEAN)
                buf = null;
        }

        return curBufItem != null;
    }

    /** */
    public void mode(BufferedMode mode) {
        this.mode = mode;

        if (mode == BufferedMode.STORE && buf == null)
            buf = new ArrayList<>();
    }

    /** */
    public void txVerFilter(IgnitePredicate<GridCacheVersion> filter) {
        txVerFilter = filter;
    }

    /** Applies {@link #txVerFilter} for WAL records. */
    private boolean walRecordFilter(WALRecord rec) {
        if (txVerFilter == null)
            return true;

        if (rec.type() == WALRecord.RecordType.DATA_RECORD_V2) {
            DataRecord dataRec = (DataRecord)rec;

            DataEntry entry = dataRec.writeEntries().get(0);

            return txVerFilter.apply(entry.nearXidVersion());
        }
        else if (rec.type() == WALRecord.RecordType.TX_RECORD)
            return txVerFilter.apply(((TxRecord)rec).nearXidVersion());

        return true;
    }

    /** Reset buffer before read from it. */
    public void resetBuffer() {
        if (buf != null)
            bufIt = new ArrayList<>(buf).iterator();
    }

    /** */
    public List<IgniteBiTuple<WALPointer, WALRecord>> buffer() {
        return buf;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        walIt.close();

        buf = null;
        bufIt = null;
    }

    /** {@inheritDoc} */
    @Override public Optional<WALPointer> lastRead() {
        return Optional.ofNullable(lastReadPtr);
    }

    /** {@inheritDoc} */
    @Override public void removeX() {
        throw new IllegalStateException("Should not be invoked");
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        throw new IllegalStateException("Should not be invoked");
    }

    /** Buffer modes. */
    public enum BufferedMode {
        /** Do not store new read records from WAL. */
        CLEAN,

        /** Store records new read records from WAL to internal buffer. */
        STORE
    }
}
