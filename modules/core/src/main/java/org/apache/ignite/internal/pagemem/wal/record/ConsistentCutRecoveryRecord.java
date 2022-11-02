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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 */
public class ConsistentCutRecoveryRecord extends WALRecord {
    /**
     * Consistent Cut Version.
     */
    @GridToStringInclude
    private final ConsistentCutVersion ver;

    /** */
    public ConsistentCutRecoveryRecord(ConsistentCutVersion ver) {
        this.ver = ver;
    }

    /** */
    public ConsistentCutVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CONSISTENT_CUT_RECOVERY_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConsistentCutRecoveryRecord.class, this);
    }
}
