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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.nio.ByteBuffer;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Job cancellation request.
 */
public class GridJobCancelRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid sessionId;

    /** */
    private IgniteUuid jobId;

    /** */
    private boolean system;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     * This constructor is not meant to be used for other purposes.
     */
    public GridJobCancelRequest() {
        // No-op.
    }

    /**
     * @param sessionId Task session ID.
     */
    public GridJobCancelRequest(IgniteUuid sessionId) {
        assert sessionId != null;

        this.sessionId = sessionId;
    }

    /**
     * @param sessionId Task session ID.
     * @param jobId Job ID.
     */
    public GridJobCancelRequest(@Nullable IgniteUuid sessionId, @Nullable IgniteUuid jobId) {
        assert sessionId != null || jobId != null;

        this.sessionId = sessionId;
        this.jobId = jobId;
    }

    /**
     * @param sessionId Session ID.
     * @param jobId Job ID.
     * @param system System flag.
     */
    public GridJobCancelRequest(@Nullable IgniteUuid sessionId, @Nullable IgniteUuid jobId, boolean system) {
        assert sessionId != null || jobId != null;

        this.sessionId = sessionId;
        this.jobId = jobId;
        this.system = system;
    }

    /**
     * Gets execution ID of task to be cancelled.
     *
     * @return Execution ID of task to be cancelled.
     */
    @Order(0)
    @Nullable public IgniteUuid sessionId() {
        return sessionId;
    }

    /**
     * Sets execution ID of task to be cancelled.
     */
    public void sessionId(@Nullable IgniteUuid sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * Gets session ID of job to be cancelled. If {@code null}, then
     * all jobs for the specified task execution ID will be cancelled.
     *
     * @return Execution ID of job to be cancelled.
     */
    @Order(0)
    @Nullable public IgniteUuid jobId() {
        return jobId;
    }

    /**
     * Gets session ID of job to be cancelled. If {@code null}, then
     * all jobs for the specified task execution ID will be cancelled.
     */
    public void jobId(@Nullable IgniteUuid jobId) {
        this.jobId = jobId;
    }

    /**
     * @return {@code True} if request to cancel is sent out of system when task
     *       has already been reduced and further results are no longer interesting.
     */
    @Order(0)
    public boolean system() {
        return system;
    }

    /**
     * @param system --
     */
    public void system(boolean system) {
        this.system = system;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return GridJobCancelRequestSerializer.writeTo(this, buf, writer);
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return GridJobCancelRequestSerializer.readFrom(this, buf, reader);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobCancelRequest.class, this);
    }

    /** */
    private static class GridJobCancelRequestSerializer {
        /**
         *
         */
        public static boolean writeTo(GridJobCancelRequest msg, ByteBuffer buf, MessageWriter writer) {
            writer.setBuffer(buf);

            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(msg.directType()))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 0:
                    if (!writer.writeIgniteUuid("", msg.jobId()))
                        return false;

                    writer.incrementState();

                case 1:
                    if (!writer.writeIgniteUuid("", msg.sessionId()))
                        return false;

                    writer.incrementState();

                case 2:
                    if (!writer.writeBoolean("", msg.system()))
                        return false;

                    writer.incrementState();

            }

            return true;
        }

        /**
         *
         */
        public static boolean readFrom(GridJobCancelRequest msg, ByteBuffer buf, MessageReader reader) {
            reader.setBuffer(buf);

            if (!reader.beforeMessageRead())
                return false;

            switch (reader.state()) {
                case 0:
                    msg.jobId(reader.readIgniteUuid(""));

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                case 1:
                    msg.sessionId(reader.readIgniteUuid(""));

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                case 2:
                    msg.system(reader.readBoolean(""));

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

            }

            return reader.afterMessageRead(GridJobCancelRequest.class);
        }
    }
}
