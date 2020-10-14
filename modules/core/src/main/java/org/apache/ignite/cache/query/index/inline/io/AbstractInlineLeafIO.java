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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * Inner page for cache row references.
 */
public abstract class AbstractInlineLeafIO extends BPlusLeafIO<InlinedIndexSearchRow> {
    /** Size of indexed keys. */
    protected final int idxKeysSize;

    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param idxKeysSize size of calculated inline keys.
     */
    AbstractInlineLeafIO(short type, int ver, int idxKeysSize) {
        super(type, ver, 8 + idxKeysSize);

        this.idxKeysSize = idxKeysSize;
    }

    /**
     */
    public static void register() {
        short type = PageIO.T_H2_EX_REF_LEAF_START;

        for (short payload = 1; payload <= PageIO.MAX_PAYLOAD_SIZE; payload++) {
            IOVersions<? extends AbstractInlineLeafIO> io =
                new IOVersions<AbstractInlineLeafIO>(
                    new InlineLeafIO((short)(type + payload - 1), payload));

            PageIO.registerH2ExtraLeaf(io, false);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public final void storeByOffset(long pageAddr, int off, InlinedIndexSearchRow row) {
        assert row.link() != 0 : row;

        int fieldOff = 0;

        for (int i = 0; i < row.getSchema().length; i++) {
            try {
//                byte[] data = definition.getContext().marshaller().marshal(row.getIdxKey(i));
//                int size = data.length;

                int maxSize = idxKeysSize - fieldOff;

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
        PageUtils.putLong(pageAddr, off + idxKeysSize, row.link());
    }

    /** {@inheritDoc} */
    @Override public final InlinedIndexSearchRow getLookupRow(BPlusTree<InlinedIndexSearchRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {

        long link = PageUtils.getLong(pageAddr, offset(idx) + idxKeysSize);

        assert link != 0;

        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        // TODO: check cache
        return new InlinedIndexRow(row);
    }

    /** {@inheritDoc} */
    @Override public final void store(long dstPageAddr, int dstIdx, BPlusIO<InlinedIndexSearchRow> srcIo, long srcPageAddr, int srcIdx) {
        int srcOff = srcIo.offset(srcIdx);

        byte[] payload = PageUtils.getBytes(srcPageAddr, srcOff, idxKeysSize);
        long link = PageUtils.getLong(srcPageAddr, srcOff + idxKeysSize);

        assert link != 0;

        int dstOff = offset(dstIdx);

        PageUtils.putBytes(dstPageAddr, dstOff, payload);
        PageUtils.putLong(dstPageAddr, dstOff + idxKeysSize, link);
    }
}
