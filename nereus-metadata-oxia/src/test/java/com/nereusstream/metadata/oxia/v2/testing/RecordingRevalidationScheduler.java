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

package com.nereusstream.metadata.oxia.v2.testing;

import com.nereusstream.metadata.oxia.v2.continuity.RevalidationScheduler;
import java.util.ArrayList;
import java.util.List;

/** Non-blocking scheduler fake recording exact generation/epoch requests. */
public final class RecordingRevalidationScheduler implements RevalidationScheduler {
    public record Request(long generation, long invalidationEpoch) {}

    private final List<Request> requests = new ArrayList<>();
    private final List<String> lifecycleEvents;
    private boolean accept = true;
    private boolean closed;

    public RecordingRevalidationScheduler() {
        this(new ArrayList<>());
    }

    public RecordingRevalidationScheduler(List<String> lifecycleEvents) {
        this.lifecycleEvents = lifecycleEvents;
    }

    public void rejectRequests() {
        accept = false;
    }

    public List<Request> requests() {
        return List.copyOf(requests);
    }

    public boolean closed() {
        return closed;
    }

    @Override
    public boolean request(long clientGeneration, long invalidationEpoch) {
        requests.add(new Request(clientGeneration, invalidationEpoch));
        return accept && !closed;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            lifecycleEvents.add("scheduler-close");
        }
    }
}
