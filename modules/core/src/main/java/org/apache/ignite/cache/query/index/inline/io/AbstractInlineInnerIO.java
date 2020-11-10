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

package org.apache.ignite.cache.query.index.inline.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;


/**
 * Inner page for cache row references.
 *
 */
// Those IOs are shared between multiple indexes. So can't store schema there.
public abstract class AbstractInlineInnerIO extends BPlusInnerIO<InlinedIndexSearchRow> {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param idxKeysSize size of calculated inline keys.
     */
    AbstractInlineInnerIO(short type, int ver, int idxKeysSize) {
        super(type, ver, true, 8 + idxKeysSize);

        // TODO: BPlusIO contains information about itemSize, so we can calculate idxKeySize from it.
        //          getItemSize() - 8 == idxKeysSize
    }

    /**
     */
    public static void register() {
        short type = PageIO.T_H2_EX_REF_INNER_START;

        for (short payload = 1; payload <= PageIO.MAX_PAYLOAD_SIZE; payload++) {
            IOVersions<? extends AbstractInlineInnerIO> io =
                new IOVersions<AbstractInlineInnerIO>(
                    new InlineInnerIO((short)(type + payload - 1), payload));

            PageIO.registerH2ExtraInner(io, false);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public final void storeByOffset(long pageAddr, int off, InlinedIndexSearchRow row) {
        assert row.link() != 0 : row;

        // TODO: inline size from row?

        int fieldOff = 0;

        for (int i = 0; i < row.getSchema().length; i++) {
            try {
                int maxSize = inlineSize() - fieldOff;

//                if (size > 0 && size + 1 > maxSize)
//                    break;  // for

                // PageUtils.putBytes(pageAddr, off + fieldOff, data);
                int size = row.getSchema()[i].put(pageAddr, off + fieldOff, row.idxKeys()[i], maxSize);

                fieldOff += size;

            } catch (Exception e) {
                throw new IgniteException("Failed to store new index row.", e);
            }
        }

        // Write link after all inlined idx keys.
        PageUtils.putLong(pageAddr, off + inlineSize(), row.link());
    }

    /** {@inheritDoc} */
    @Override public final InlinedIndexSearchRow getLookupRow(BPlusTree<InlinedIndexSearchRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        // TODO: inline size got from tree?

        long link = PageUtils.getLong(pageAddr, offset(idx) + inlineSize());

        assert link != 0;

        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        // TODO: check cache
        return new InlinedIndexRow(row);
    }

    /** {@inheritDoc} */
    @Override public final void store(long dstPageAddr, int dstIdx, BPlusIO<InlinedIndexSearchRow> srcIo, long srcPageAddr, int srcIdx) {
        int srcOff = srcIo.offset(srcIdx);

        byte[] payload = PageUtils.getBytes(srcPageAddr, srcOff, inlineSize());
        long link = PageUtils.getLong(srcPageAddr, srcOff + inlineSize());

        assert link != 0;

        int dstOff = offset(dstIdx);

        PageUtils.putBytes(dstPageAddr, dstOff, payload);
        PageUtils.putLong(dstPageAddr, dstOff + inlineSize(), link);
    }

    /** */
    // TODO: variable provide a cache for this function. So rollback to idxKeySize fields
    private int inlineSize() {
        return getItemSize() - 8;
    }
}
