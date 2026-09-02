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

package com.nereusstream.storage.object.read;

/** Caller-owned bounded plan storage; reset/fill performs no heap allocation. */
public final class BindingReadPlanBufferV1 {
    private final long[] starts;
    private final long[] ends;
    private final BindingReadRouteV1[] routes;
    private int size;

    public BindingReadPlanBufferV1(int capacity) {
        if (capacity <= 0 || capacity > 256) {
            throw new IllegalArgumentException("read plan capacity must be in [1,256]");
        }
        starts = new long[capacity];
        ends = new long[capacity];
        routes = new BindingReadRouteV1[capacity];
    }

    void reset() {
        for (int index = 0; index < size; index++) {
            routes[index] = null;
        }
        size = 0;
    }

    boolean append(long startInclusive, long endExclusive, BindingReadRouteV1 route) {
        if (size == routes.length) {
            return false;
        }
        starts[size] = startInclusive;
        ends[size] = endExclusive;
        routes[size] = route;
        size++;
        return true;
    }

    public int size() {
        return size;
    }

    public long startInclusive(int index) {
        checkIndex(index);
        return starts[index];
    }

    public long endExclusive(int index) {
        checkIndex(index);
        return ends[index];
    }

    public BindingReadRouteV1 route(int index) {
        checkIndex(index);
        return routes[index];
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(index);
        }
    }
}
