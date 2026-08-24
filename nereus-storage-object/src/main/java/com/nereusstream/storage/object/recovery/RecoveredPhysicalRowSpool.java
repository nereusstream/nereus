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

package com.nereusstream.storage.object.recovery;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.control.ProviderProofMode;
import com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/** Fixed-width proof-NONE physical-row spool. It never retains a per-row Java object graph. */
final class RecoveredPhysicalRowSpool implements AutoCloseable {
    static final int ROW_BYTES = WalRunControlCodec.proofNoneCheckpointRowCanonicalLength();

    private enum State {
        CHECKPOINT,
        STRONG_LIST,
        READY,
        CONSUMED,
        CLOSED
    }

    private final byte[] bytes;
    private final int capacityRows;
    private final int[] laneStarts = {-1, -1, -1};
    private final int[] laneCounts = {0, 0, 0};
    private final long[] laneThrough = {-1, -1, -1};
    private int checkpointRows;
    private int checkpointStart;
    private int checkpointCursor;
    private int finalRows;
    private int currentLane;
    private long lastListedSequence = -1;
    private boolean authenticated;
    private State state = State.CHECKPOINT;

    RecoveredPhysicalRowSpool(long maximumRows) {
        if (maximumRows <= 0 || maximumRows > Integer.MAX_VALUE / ROW_BYTES) {
            throw new IllegalArgumentException("physical-row spool cap is outside the Java array domain");
        }
        capacityRows = Math.toIntExact(maximumRows);
        bytes = new byte[Math.toIntExact(Math.multiplyExact(maximumRows, ROW_BYTES))];
    }

    void appendCheckpoint(ProviderResolvedExtentRowV1 row) {
        requireState(State.CHECKPOINT);
        if (checkpointRows == capacityRows) {
            throw new IllegalStateException("physical checkpoint exceeds the exact Root extent cap");
        }
        writeRow(checkpointRows, row);
        checkpointRows = Math.incrementExact(checkpointRows);
    }

    /** Sorts the backwards checkpoint stream in place and moves it behind the maximum uncovered-suffix slack. */
    void beginStrongListFold() {
        requireState(State.CHECKPOINT);
        heapSort(checkpointRows);
        for (int index = 1; index < checkpointRows; index++) {
            if (compareRows(index - 1, index) == 0) {
                throw new IllegalStateException("physical checkpoint contains a duplicate lane sequence");
            }
        }
        checkpointStart = Math.subtractExact(capacityRows, checkpointRows);
        if (checkpointRows > 0 && checkpointStart > 0) {
            System.arraycopy(bytes, 0, bytes, checkpointStart * ROW_BYTES, checkpointRows * ROW_BYTES);
        }
        checkpointCursor = 0;
        finalRows = 0;
        currentLane = 0;
        laneStarts[0] = 0;
        state = State.STRONG_LIST;
    }

    /** Folds one globally ordered strong-LIST row against the exact physical checkpoint. */
    void acceptListedRow(WalLaneId laneId, ProviderResolvedExtentRowV1 listedRow, long coveredThrough) {
        requireState(State.STRONG_LIST);
        Objects.requireNonNull(laneId, "laneId");
        requireProofNone(listedRow);
        if (laneId.code() != currentLane || listedRow.laneId() != laneId) {
            throw new IllegalStateException("strong LIST row is outside the current permanent lane");
        }
        long expectedSequence = Math.incrementExact(lastListedSequence);
        if (listedRow.laneSequence() != expectedSequence) {
            throw new IllegalStateException("strong LIST lane sequence is not contiguous from zero");
        }
        CanonicalBytes encoded = encodeProofNone(listedRow);
        if (listedRow.laneSequence() <= coveredThrough) {
            if (checkpointCursor == checkpointRows || !rowEquals(checkpointStart + checkpointCursor, encoded)) {
                throw new IllegalStateException("strong LIST checkpoint row differs from the exact physical row");
            }
            checkpointCursor = Math.incrementExact(checkpointCursor);
        } else if (checkpointCursor < checkpointRows && laneCode(checkpointStart + checkpointCursor) == currentLane) {
            throw new IllegalStateException("physical checkpoint contains a row above its covered-through vector");
        }
        if (finalRows == capacityRows) {
            throw new IllegalStateException("strong LIST inventory exceeds the exact Root extent cap");
        }
        writeEncoded(finalRows, encoded);
        finalRows = Math.incrementExact(finalRows);
        lastListedSequence = listedRow.laneSequence();
    }

    void endLane(WalLaneId laneId, long coveredThrough) {
        requireState(State.STRONG_LIST);
        Objects.requireNonNull(laneId, "laneId");
        if (laneId.code() != currentLane || lastListedSequence < coveredThrough) {
            throw new IllegalStateException("strong LIST lane does not cover the physical checkpoint vector");
        }
        if (checkpointCursor < checkpointRows && laneCode(checkpointStart + checkpointCursor) == currentLane) {
            throw new IllegalStateException("strong LIST omitted a physical-checkpoint row");
        }
        laneCounts[currentLane] = Math.subtractExact(finalRows, laneStarts[currentLane]);
        laneThrough[currentLane] = lastListedSequence;
        currentLane = Math.incrementExact(currentLane);
        lastListedSequence = -1;
        if (currentLane < WalLaneId.values().length) {
            laneStarts[currentLane] = finalRows;
        }
    }

    void finishStrongListFold() {
        requireState(State.STRONG_LIST);
        if (currentLane != WalLaneId.values().length || checkpointCursor != checkpointRows) {
            throw new IllegalStateException("strong LIST fold is incomplete");
        }
        Arrays.fill(bytes, finalRows * ROW_BYTES, bytes.length, (byte) 0);
        state = State.READY;
    }

    /** Internal verification pass; callers must authenticate every row before sealing the cut. */
    void verifyRows(RowConsumer consumer) throws IOException {
        requireState(State.READY);
        if (authenticated) {
            throw new IllegalStateException("physical-row spool was already authenticated");
        }
        consumeRows(Objects.requireNonNull(consumer, "consumer"));
        authenticated = true;
    }

    /** One-use protocol fold. A callback failure burns this cut rather than replaying partially staged effects. */
    void consumeAuthenticatedRows(RowConsumer consumer) throws IOException {
        requireState(State.READY);
        if (!authenticated) {
            throw new IllegalStateException("physical-row spool has not completed authenticated prefix verification");
        }
        state = State.CONSUMED;
        consumeRows(Objects.requireNonNull(consumer, "consumer"));
    }

    int rowCount() {
        requireNotClosed();
        return finalRows;
    }

    int laneStart(WalLaneId laneId) {
        requireNotClosed();
        return laneStarts[Objects.requireNonNull(laneId, "laneId").code()];
    }

    int laneCount(WalLaneId laneId) {
        requireNotClosed();
        return laneCounts[Objects.requireNonNull(laneId, "laneId").code()];
    }

    long laneThrough(WalLaneId laneId) {
        requireNotClosed();
        return laneThrough[Objects.requireNonNull(laneId, "laneId").code()];
    }

    boolean authenticated() {
        requireNotClosed();
        return authenticated;
    }

    @Override
    public void close() {
        if (state == State.CLOSED) {
            return;
        }
        Arrays.fill(bytes, (byte) 0);
        state = State.CLOSED;
    }

    private void consumeRows(RowConsumer consumer) throws IOException {
        for (int index = 0; index < finalRows; index++) {
            consumer.accept(WalRunControlCodec.decodeCheckpointRow(readEncoded(index)));
        }
    }

    private void writeRow(int index, ProviderResolvedExtentRowV1 row) {
        writeEncoded(index, encodeProofNone(row));
    }

    private static CanonicalBytes encodeProofNone(ProviderResolvedExtentRowV1 row) {
        requireProofNone(row);
        CanonicalBytes encoded = WalRunControlCodec.encodeCheckpointRow(row);
        if (encoded.length() != ROW_BYTES) {
            throw new IllegalStateException("proof-NONE physical row is not the fixed 56-byte canonical wire");
        }
        return encoded;
    }

    private static void requireProofNone(ProviderResolvedExtentRowV1 row) {
        Objects.requireNonNull(row, "row");
        if (row.providerProof().mode() != ProviderProofMode.NONE
                || !row.providerProof().canonicalVersionToken().isEmpty()) {
            throw new IllegalStateException("M3 production physical-row spool admits proof NONE only");
        }
    }

    private void writeEncoded(int index, CanonicalBytes encoded) {
        byte[] exact = encoded.toByteArray();
        System.arraycopy(exact, 0, bytes, index * ROW_BYTES, ROW_BYTES);
    }

    private CanonicalBytes readEncoded(int index) {
        return CanonicalBytes.copyOf(Arrays.copyOfRange(bytes, index * ROW_BYTES, (index + 1) * ROW_BYTES));
    }

    private boolean rowEquals(int index, CanonicalBytes expected) {
        byte[] exact = expected.toByteArray();
        int offset = index * ROW_BYTES;
        for (int byteIndex = 0; byteIndex < ROW_BYTES; byteIndex++) {
            if (bytes[offset + byteIndex] != exact[byteIndex]) {
                return false;
            }
        }
        return true;
    }

    private void heapSort(int count) {
        for (int root = count / 2 - 1; root >= 0; root--) {
            siftDown(root, count);
        }
        for (int end = count - 1; end > 0; end--) {
            swapRows(0, end);
            siftDown(0, end);
        }
    }

    private void siftDown(int root, int count) {
        int candidate = root;
        while (true) {
            int left = Math.addExact(Math.multiplyExact(candidate, 2), 1);
            if (left >= count) {
                return;
            }
            int largest = left;
            int right = Math.incrementExact(left);
            if (right < count && compareRows(left, right) < 0) {
                largest = right;
            }
            if (compareRows(candidate, largest) >= 0) {
                return;
            }
            swapRows(candidate, largest);
            candidate = largest;
        }
    }

    private int compareRows(int leftIndex, int rightIndex) {
        int laneComparison = Integer.compare(laneCode(leftIndex), laneCode(rightIndex));
        return laneComparison != 0 ? laneComparison : Long.compare(laneSequence(leftIndex), laneSequence(rightIndex));
    }

    private int laneCode(int index) {
        return Byte.toUnsignedInt(bytes[index * ROW_BYTES]);
    }

    private long laneSequence(int index) {
        int offset = index * ROW_BYTES + 1;
        long value = 0;
        for (int indexWithinLong = 0; indexWithinLong < Long.BYTES; indexWithinLong++) {
            value = (value << 8) | Byte.toUnsignedLong(bytes[offset + indexWithinLong]);
        }
        return value;
    }

    private void swapRows(int leftIndex, int rightIndex) {
        if (leftIndex == rightIndex) {
            return;
        }
        int left = leftIndex * ROW_BYTES;
        int right = rightIndex * ROW_BYTES;
        for (int byteIndex = 0; byteIndex < ROW_BYTES; byteIndex++) {
            byte value = bytes[left + byteIndex];
            bytes[left + byteIndex] = bytes[right + byteIndex];
            bytes[right + byteIndex] = value;
        }
    }

    private void requireState(State expected) {
        if (state != expected) {
            throw new IllegalStateException("physical-row spool state is " + state + ", expected " + expected);
        }
    }

    private void requireNotClosed() {
        if (state == State.CLOSED) {
            throw new IllegalStateException("physical-row spool is closed");
        }
    }

    @FunctionalInterface
    interface RowConsumer {
        void accept(ProviderResolvedExtentRowV1 row) throws IOException;
    }
}
