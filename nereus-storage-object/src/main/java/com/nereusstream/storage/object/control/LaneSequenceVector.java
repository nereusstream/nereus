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

package com.nereusstream.storage.object.control;

import java.util.Arrays;

/** Three-component sequence vector. A component of -1 means the lane was never instantiated. */
public final class LaneSequenceVector {
    private final long[] values;

    private LaneSequenceVector(long[] values) {
        this.values = values;
    }

    public static LaneSequenceVector of(long latency, long balanced, long cost) {
        long[] values = new long[] {latency, balanced, cost};
        for (long value : values) {
            if (value < -1) {
                throw new IllegalArgumentException("lane vector components must be -1 or non-negative");
            }
        }
        return new LaneSequenceVector(values);
    }

    public static LaneSequenceVector empty() {
        return of(-1, -1, -1);
    }

    public long get(WalLaneId laneId) {
        return values[laneId.code()];
    }

    public LaneSequenceVector with(WalLaneId laneId, long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("instantiated lane sequence must be non-negative");
        }
        long previous = get(laneId);
        if (previous >= 0 && sequence < previous) {
            throw new IllegalArgumentException("lane sequence vector may not regress");
        }
        long[] copy = values.clone();
        copy[laneId.code()] = sequence;
        return new LaneSequenceVector(copy);
    }

    public boolean componentWiseAtLeast(LaneSequenceVector predecessor) {
        for (int index = 0; index < values.length; index++) {
            if (values[index] < predecessor.values[index]) {
                return false;
            }
        }
        return true;
    }

    public long[] toArray() {
        return values.clone();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof LaneSequenceVector that && Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }
}
