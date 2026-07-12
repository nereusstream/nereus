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

package com.nereusstream.managedledger.projection;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import java.util.Map;
import java.util.Objects;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;

/** Role-specific conversion between Pulsar Positions and F2 stream offsets. */
public final class PositionProjection {
    private static final Map<String, String> REQUIRED_ATTRIBUTES = Map.of(
            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1);

    public StreamPositionBounds bounds(
            VirtualLedgerProjection projection,
            StreamMetadata snapshot) {
        validateProjectionSnapshot(projection, snapshot);
        long ledgerId = projection.virtualLedgerId();
        return new StreamPositionBounds(
                snapshot.trimOffset(),
                snapshot.committedEndOffset(),
                PositionFactory.create(ledgerId, snapshot.trimOffset() - 1),
                PositionFactory.create(ledgerId, snapshot.trimOffset()),
                PositionFactory.create(ledgerId, snapshot.committedEndOffset() - 1),
                PositionFactory.create(ledgerId, snapshot.committedEndOffset()));
    }

    public Position entryPosition(VirtualLedgerProjection projection, long offset) {
        Objects.requireNonNull(projection, "projection");
        if (offset < 0 || offset >= Long.MAX_VALUE) {
            throw new ProjectionValidationException("entry offset is outside the F2 range");
        }
        return PositionFactory.create(projection.virtualLedgerId(), offset);
    }

    public long requireReadableEntryOffset(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata snapshot) {
        StreamPositionBounds current = bounds(projection, snapshot);
        long entryId = requireCurrentLedger(projection, position);
        if (entryId < current.trimOffset() || entryId >= current.committedEndOffset()) {
            throw new ProjectionValidationException("position is trimmed, at tail, or beyond tail");
        }
        return entryId;
    }

    public long requireReadPositionOffset(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata snapshot) {
        StreamPositionBounds current = bounds(projection, snapshot);
        long entryId = requireCurrentLedger(projection, position);
        if (entryId < current.trimOffset() || entryId > current.committedEndOffset()) {
            throw new ProjectionValidationException("read position is outside the retained/tail range");
        }
        return entryId;
    }

    public long markDeleteOffsetAfter(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata snapshot) {
        StreamPositionBounds current = bounds(projection, snapshot);
        long entryId = requireCurrentLedger(projection, position);
        long beforeFirst = current.trimOffset() - 1;
        long lastConfirmed = current.committedEndOffset() - 1;
        if (entryId < beforeFirst || entryId > lastConfirmed) {
            throw new ProjectionValidationException("mark-delete position is outside the retained range");
        }
        try {
            return Math.addExact(entryId, 1);
        } catch (ArithmeticException e) {
            throw new ProjectionValidationException("mark-delete offset overflows", e);
        }
    }

    public Position readPosition(
            VirtualLedgerProjection projection,
            long nextOffset,
            StreamMetadata snapshot) {
        StreamPositionBounds current = bounds(projection, snapshot);
        if (nextOffset < current.trimOffset() || nextOffset > current.committedEndOffset()) {
            throw new ProjectionValidationException("next read offset is outside the retained/tail range");
        }
        return PositionFactory.create(projection.virtualLedgerId(), nextOffset);
    }

    public Position normalizeInclusiveMaxPosition(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata snapshot) {
        StreamPositionBounds current = bounds(projection, snapshot);
        if (position == null || samePosition(position, PositionFactory.LATEST)) {
            return current.lastConfirmed();
        }
        if (samePosition(position, PositionFactory.EARLIEST)) {
            return current.beforeFirstAvailable();
        }
        long entryId = requireCurrentLedger(projection, position);
        if (entryId < -1) {
            throw new ProjectionValidationException("inclusive max entryId cannot be less than -1");
        }
        if (entryId < current.trimOffset()) {
            return current.beforeFirstAvailable();
        }
        if (entryId >= current.committedEndOffset()) {
            return current.lastConfirmed();
        }
        return PositionFactory.create(projection.virtualLedgerId(), entryId);
    }

    private static void validateProjectionSnapshot(
            VirtualLedgerProjection projection,
            StreamMetadata snapshot) {
        Objects.requireNonNull(projection, "projection");
        Objects.requireNonNull(snapshot, "snapshot");
        if (!projection.streamId().equals(snapshot.streamId())) {
            throw new ProjectionValidationException("projection and stream snapshot have different stream IDs");
        }
        if (!ManagedLedgerProjectionNames.streamName(
                        projection.managedLedgerName(), projection.incarnation())
                .equals(snapshot.streamName())) {
            throw new ProjectionValidationException("projection and stream snapshot have different stream names");
        }
        if (snapshot.profile().canonical() != StorageProfile.OBJECT_WAL_SYNC_OBJECT) {
            throw new ProjectionValidationException("F2 requires OBJECT_WAL_SYNC_OBJECT");
        }
        if (!snapshot.attributes().equals(REQUIRED_ATTRIBUTES)) {
            throw new ProjectionValidationException("stream payload mapping attributes do not match F2");
        }
    }

    private static long requireCurrentLedger(
            VirtualLedgerProjection projection,
            Position position) {
        Objects.requireNonNull(position, "position");
        if (position.getLedgerId() != projection.virtualLedgerId()) {
            throw new ProjectionValidationException("position belongs to a different virtual ledger");
        }
        return position.getEntryId();
    }

    private static boolean samePosition(Position left, Position right) {
        return left.getLedgerId() == right.getLedgerId() && left.getEntryId() == right.getEntryId();
    }
}
