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

import org.apache.ignite.cache.query.index.inline.InlineIndexKeyType;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index column implementation for inlining {@link Integer} values.
 */
public class IntegerInlineIndexKey extends NullableInlineIndexKey<Integer> {

    public IntegerInlineIndexKey() {
        super(InlineIndexKeyType.INT, (short) 4);
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Integer val, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte) type());
        // +1 shift after type
        PageUtils.putInt(pageAddr, off + 1, val);

        // TODO: replace with explicit 4 + 1
        return keySize() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Integer get0(long pageAddr, int off) {
        // +1 shift after type
        return PageUtils.getInt(pageAddr, off + 1);
    }
}
