/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.managedledger.entry;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.EntryReadCountHandler;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/** An immutable-position Pulsar Entry owning one read-only Netty buffer reference. */
public final class NereusEntry implements Entry {
    private final Position position;
    private final ByteBuf data;
    private final AtomicBoolean released = new AtomicBoolean();
    private boolean messageMetadataParsed;
    private MessageMetadata messageMetadata;

    public NereusEntry(Position position, byte[] payload) {
        this.position = PositionFactory.create(Objects.requireNonNull(position, "position"));
        if (position.getEntryId() < 0) {
            throw new IllegalArgumentException("entry position must have a non-negative entryId");
        }
        byte[] ownedPayload = Objects.requireNonNull(payload, "payload").clone();
        this.data = Unpooled.wrappedBuffer(ownedPayload).asReadOnly();
    }

    @Override
    public synchronized byte[] getData() {
        ensureAccessible();
        byte[] result = new byte[data.readableBytes()];
        data.getBytes(data.readerIndex(), result);
        return result;
    }

    @Override
    public synchronized byte[] getDataAndRelease() {
        byte[] result = getData();
        release();
        return result;
    }

    @Override
    public synchronized int getLength() {
        ensureAccessible();
        return data.readableBytes();
    }

    @Override
    public synchronized ByteBuf getDataBuffer() {
        ensureAccessible();
        return data;
    }

    @Override
    public Position getPosition() {
        return position;
    }

    @Override
    public long getLedgerId() {
        return position.getLedgerId();
    }

    @Override
    public long getEntryId() {
        return position.getEntryId();
    }

    @Override
    public synchronized boolean release() {
        if (!released.compareAndSet(false, true)) {
            return false;
        }
        return data.release();
    }

    @Override
    public EntryReadCountHandler getReadCountHandler() {
        return null;
    }

    @Override
    public synchronized MessageMetadata getMessageMetadata() {
        ensureAccessible();
        if (!messageMetadataParsed) {
            messageMetadataParsed = true;
            try {
                MessageMetadata parsed = new MessageMetadata();
                Commands.parseMessageMetadata(data.duplicate(), parsed);
                messageMetadata = parsed;
            } catch (Throwable ignored) {
                messageMetadata = null;
            }
        }
        return messageMetadata;
    }

    private void ensureAccessible() {
        if (released.get()) {
            throw new IllegalStateException("entry has been released");
        }
    }
}
