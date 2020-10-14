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

package org.apache.ignite.cache.query.index.inline.keys;

import org.apache.ignite.cache.query.index.inline.InlineIndexKey;
import org.apache.ignite.cache.query.index.inline.InlineIndexKeyType;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract inline column.
 */
public abstract class NullableInlineIndexKey<K> implements InlineIndexKey<K> {
    /** Value for comparison meaning 'Not enough information to compare'. */
    public static final int CANT_BE_COMPARE = -2;

    /** Value for comparison meaning 'Compare not supported for given value'. */
    public static final int COMPARE_UNSUPPORTED = Integer.MIN_VALUE;

    /** */
    private final InlineIndexKeyType type;

    /** */
    private final short size;

    /**
     * @param type Index key type.
     * @param size Size.
     */
    protected NullableInlineIndexKey(InlineIndexKeyType type, short size) {
        this.type = type;
        this.size = size;
    }

    /** {@inheritDoc} */
    @Override public int type() {
        return type.getType();
    }

    /** {@inheritDoc} */
    @Override public short keySize() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public int size(long pageAddr, int off) {
        int type = PageUtils.getByte(pageAddr, off);

        if (type == InlineIndexKeyType.NULL.getType())
            return 1;

        // TODO: +1, +3?
        if (size > 0)
            return size + 1;
        else
            return (PageUtils.getShort(pageAddr, off + 1) & 0x7FFF) + 3;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        if (type == InlineIndexKeyType.NULL)
            return 1;

        // TODO: +1, +3?
        return size + 1;
    }

    /**
     * Restores value from inline, if possible.
     *
     * @param pageAddr Address of the page.
     * @param off Offset on the page.
     * @param maxSize Maxim size. TODO?
     *
     * @return Restored value or {@code null} if value can't be restored.
     */
    @Override public K get(long pageAddr, int off, int maxSize) {
        if (size > 0 && size + 1 > maxSize)
            return null;

        if (maxSize < 1)
            return null;

        int type = PageUtils.getByte(pageAddr, off);

        if (type == InlineIndexKeyType.UNKNOWN.getType())
            return null;

        // TODO: how to differ all nulls?
        if (type == InlineIndexKeyType.NULL.getType())
            return null;

        checkValueType(type);

        return get0(pageAddr, off);
    }

    /** {@inheritDoc} */
    @Override public int put(long pageAddr, int off, K val, int maxSize) {
        // +1 is a length of the type byte.
        if (size > 0 && size + 1 > maxSize)
            return 0;

        if (size < 0 && maxSize < 4) {
            // Can't fit vartype field.
            PageUtils.putByte(pageAddr, off, (byte) InlineIndexKeyType.UNKNOWN.getType());
            return 0;
        }

        if (val == null) {
            PageUtils.putByte(pageAddr, off, (byte) InlineIndexKeyType.NULL.getType());
            return 1;
        }

        // TODO: do we need check it?
        // checkValueType(valType);

        return put0(pageAddr, off, val, maxSize);
    }

    /**
     * @param valType Value type.
     */
    private void checkValueType(int valType) {
        if (valType != type.getType())
            throw new UnsupportedOperationException("Value type doesn't match: exp=" + type + ", act=" + valType);
    }

    /**
     * Puts given value into inline index tree.
     *
     * @param pageAddr Page address.
     * @param off Offset.
     * @param val Value.
     * @param maxSize Max size.
     *
     * @return Amount of bytes actually stored.
     */
    protected abstract int put0(long pageAddr, int off, K val, int maxSize);

    /**
     * Restores value from inline.
     *
     * @param pageAddr Page address.
     * @param off Offset.
     *
     * @return Inline value or {@code null} if value can't be restored.
     */
    protected abstract @Nullable K get0(long pageAddr, int off);

    /** Read variable length bytearray */
    protected byte[] readBytes(long pageAddr, int off) {
        int size = PageUtils.getShort(pageAddr, off + 1) & 0x7FFF;
        return PageUtils.getBytes(pageAddr, off + 3, size);
    }
}
