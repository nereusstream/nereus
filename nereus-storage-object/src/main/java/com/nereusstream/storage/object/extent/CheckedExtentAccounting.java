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

package com.nereusstream.storage.object.extent;

import java.util.Objects;

/** Checked local D1 accounting. Every count/offset is validated before allocation or narrowing conversion. */
public final class CheckedExtentAccounting {
    private final ObjectWalAdmissionCaps caps;
    private int contexts;
    private int appendUnits;
    private int frames;
    private int directoryPlaintextBytes;
    private long storedBodyBytes;
    private long totalDecodedBytes;

    public CheckedExtentAccounting(ObjectWalAdmissionCaps caps) {
        this.caps = Objects.requireNonNull(caps, "caps");
    }

    public void chargeContext(int canonicalBytes) {
        int nextContexts = addCount(contexts, 1, ObjectWalFormatCaps.MAX_CONTEXTS, "Binding contexts");
        int nextDirectoryPlaintextBytes = addCount(
                directoryPlaintextBytes, canonicalBytes, caps.maximumDirectoryPlaintextBytes(), "directory plaintext");
        contexts = nextContexts;
        directoryPlaintextBytes = nextDirectoryPlaintextBytes;
    }

    public void chargeAppendUnit(int canonicalBytes) {
        int nextAppendUnits = addCount(appendUnits, 1, ObjectWalFormatCaps.MAX_APPEND_UNITS, "append units");
        int nextDirectoryPlaintextBytes = addCount(
                directoryPlaintextBytes, canonicalBytes, caps.maximumDirectoryPlaintextBytes(), "directory plaintext");
        appendUnits = nextAppendUnits;
        directoryPlaintextBytes = nextDirectoryPlaintextBytes;
    }

    public void chargeFrame(int directoryBytes, int storedBytes, int decodedBytes) {
        if (storedBytes <= 0 || storedBytes > ObjectWalFormatCaps.MAX_FRAME_STORED_BYTES) {
            throw new IllegalArgumentException("stored frame length lies outside the format bound");
        }
        if (decodedBytes < 0 || decodedBytes > ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES) {
            throw new IllegalArgumentException("decoded frame length lies outside the format bound");
        }
        int nextFrames = addCount(frames, 1, ObjectWalFormatCaps.MAX_FRAMES, "frames");
        int nextDirectoryPlaintextBytes = addCount(
                directoryPlaintextBytes, directoryBytes, caps.maximumDirectoryPlaintextBytes(), "directory plaintext");
        long nextStoredBodyBytes = addLong(storedBodyBytes, storedBytes, caps.maximumBodyBytes(), "stored Object body");
        long nextTotalDecodedBytes =
                addLong(totalDecodedBytes, decodedBytes, caps.maximumTotalDecodedBytes(), "decoded frame total");
        frames = nextFrames;
        directoryPlaintextBytes = nextDirectoryPlaintextBytes;
        storedBodyBytes = nextStoredBodyBytes;
        totalDecodedBytes = nextTotalDecodedBytes;
    }

    public void chargeFixedBodyBytes(long bytes) {
        long nextStoredBodyBytes = addLong(storedBodyBytes, bytes, caps.maximumBodyBytes(), "stored Object body");
        storedBodyBytes = nextStoredBodyBytes;
    }

    public int checkedDirectoryPrefixEnd(long fixedHeaderBytes, long encryptedDirectoryBytes) {
        long end = checkedEnd(fixedHeaderBytes, encryptedDirectoryBytes, caps.maximumDirectoryPrefixBytes());
        if (end > storedBodyBytes) {
            throw new IllegalStateException("directory prefix lies beyond the accounted Object body");
        }
        return Math.toIntExact(end);
    }

    public long checkedFrameEnd(long start, long storedFrameBytes) {
        return checkedEnd(start, storedFrameBytes, caps.maximumBodyBytes());
    }

    public Snapshot snapshot() {
        return new Snapshot(contexts, appendUnits, frames, directoryPlaintextBytes, storedBodyBytes, totalDecodedBytes);
    }

    public static long checkedEnd(long start, long length, long hardExclusiveEndCap) {
        if (start < 0 || length < 0 || hardExclusiveEndCap <= 0) {
            throw new IllegalArgumentException("offset/length/cap is invalid");
        }
        long end = Math.addExact(start, length);
        if (end > hardExclusiveEndCap) {
            throw new IllegalArgumentException("range end exceeds the admitted hard cap");
        }
        return end;
    }

    private static int addCount(int current, int delta, int maximum, String label) {
        if (delta < 0) {
            throw new IllegalArgumentException(label + " delta must be non-negative");
        }
        int candidate = Math.addExact(current, delta);
        if (candidate > maximum) {
            throw new IllegalArgumentException(label + " exceeds its hard cap");
        }
        return candidate;
    }

    private static long addLong(long current, long delta, long maximum, String label) {
        if (delta < 0) {
            throw new IllegalArgumentException(label + " delta must be non-negative");
        }
        long candidate = Math.addExact(current, delta);
        if (candidate > maximum) {
            throw new IllegalArgumentException(label + " exceeds its hard cap");
        }
        return candidate;
    }

    public record Snapshot(
            int contexts,
            int appendUnits,
            int frames,
            int directoryPlaintextBytes,
            long storedBodyBytes,
            long totalDecodedBytes) {}
}
