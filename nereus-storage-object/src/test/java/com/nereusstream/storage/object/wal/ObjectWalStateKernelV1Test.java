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

package com.nereusstream.storage.object.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Event;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.EventKind;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LaneSequenceDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LocatorPublication;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.PositionDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Protocol;
import java.util.List;
import org.junit.jupiter.api.Test;

class ObjectWalStateKernelV1Test {
    private final ObjectWalStateKernelV1 kernel = new ObjectWalStateKernelV1();

    @Test
    void replaysAllFiftyAuthoredExpectationsExactly() throws Exception {
        for (ObjectWalStateTraceV1 trace : ObjectWalStateTraceManifestV1.loadCanonical()) {
            ObjectWalStateKernelV1.Result actual = kernel.replay(trace);

            assertThat(actual.faultClass()).as(trace.traceId()).isEqualTo(trace.faultClass());
            assertThat(actual.outcome()).as(trace.traceId()).isEqualTo(trace.expectedOutcome());
            assertThat(actual.candidates()).as(trace.traceId()).containsExactlyElementsOf(trace.expectedCandidates());
            assertThat(actual.subjects()).as(trace.traceId()).containsExactlyElementsOf(trace.expectedSubjects());
            assertThat(actual.bindings()).as(trace.traceId()).containsExactlyElementsOf(trace.expectedBindings());
            assertThat(actual.lanes()).as(trace.traceId()).containsExactlyElementsOf(trace.expectedLanes());
            assertThat(actual.calls()).as(trace.traceId()).isEqualTo(trace.expectedCalls());
            assertThat(actual.budgets()).as(trace.traceId()).isEqualTo(trace.expectedBudgets());
            assertThat(actual.isolation()).as(trace.traceId()).isEqualTo(trace.expectedIsolation());
        }
    }

    @Test
    void enforcesPlanDispatchAndInjectedLaneExhaustionOrdering() throws Exception {
        ObjectWalStateTraceV1 trace =
                ObjectWalStateTraceManifestV1.loadCanonical().get(0);

        ObjectWalStateTraceV1 putBeforeDispatch = replaceEvents(
                trace, List.of(event(EventKind.PLAN, "c0", "0", "0"), event(EventKind.PUT_APPLIED, "c0")));
        assertThatThrownBy(() -> kernel.replay(putBeforeDispatch))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PUT before dispatch");

        ObjectWalStateTraceV1 earlyExhaustion =
                replaceEvents(trace, List.of(event(EventKind.PLAN, "c0", "0", "7"), event(EventKind.EXHAUST, "0")));
        assertThatThrownBy(() -> kernel.replay(earlyExhaustion))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Long.MAX_VALUE");
    }

    @Test
    void provesKafkaSuffixPulsarNoGapAndBindingIsolationCuts() throws Exception {
        List<ObjectWalStateTraceV1> traces = ObjectWalStateTraceManifestV1.loadCanonical();
        ObjectWalStateKernelV1.Result kafka = replay(traces, "K44_WHOLE_SPECULATIVE_SUFFIX_ROLLBACK");
        ObjectWalStateKernelV1.Result pulsar = replay(traces, "P48_NO_GAP_VIRTUAL_LEDGER_ROLLOVER");
        ObjectWalStateKernelV1.Result isolated = replay(traces, "P50_FRAME_FAILURE_BINDING_ISOLATION");

        assertThat(kafka.subjects()).allSatisfy(subject -> assertThat(subject.positionDisposition())
                .isEqualTo(PositionDisposition.ROLLED_BACK_SPECULATIVE_SUFFIX));
        assertThat(kafka.subjects()).allSatisfy(subject -> assertThat(subject.locatorPublication())
                .isEqualTo(LocatorPublication.ROLLED_BACK));
        assertThat(pulsar.subjects()).singleElement().satisfies(subject -> {
            assertThat(subject.positionDisposition()).isEqualTo(PositionDisposition.SEALED_BEFORE_GAP);
            assertThat(subject.ackCount()).isZero();
        });
        assertThat(isolated.subjects())
                .anySatisfy(subject -> assertThat(subject.ackCount()).isEqualTo(1));
        assertThat(isolated.subjects()).anySatisfy(subject -> assertThat(subject.positionDisposition())
                .isEqualTo(PositionDisposition.UNKNOWN_RETAINED));
        assertThat(traces.stream().filter(trace -> trace.protocol() == Protocol.KAFKA))
                .hasSize(4);
        assertThat(traces.stream().filter(trace -> trace.protocol() == Protocol.PULSAR))
                .hasSize(4);
        assertThat(replay(traces, "C12_LANE0_LONG_MAX_EXHAUSTION").lanes())
                .singleElement()
                .extracting(ObjectWalStateTraceV1.LaneState::laneSequenceDisposition)
                .isEqualTo(LaneSequenceDisposition.EXHAUSTED);
    }

    private ObjectWalStateKernelV1.Result replay(List<ObjectWalStateTraceV1> traces, String traceId) {
        return kernel.replay(traces.stream()
                .filter(trace -> trace.traceId().equals(traceId))
                .findFirst()
                .orElseThrow());
    }

    private static Event event(EventKind kind, String... arguments) {
        return new Event(kind, List.of(arguments));
    }

    private static ObjectWalStateTraceV1 replaceEvents(ObjectWalStateTraceV1 source, List<Event> events) {
        return new ObjectWalStateTraceV1(
                source.traceId(),
                source.group(),
                source.protocol(),
                source.initialState(),
                events,
                source.faultClass(),
                source.expectedOutcome(),
                source.expectedCandidates(),
                source.expectedSubjects(),
                source.expectedBindings(),
                source.expectedLanes(),
                source.expectedCalls(),
                source.expectedBudgets(),
                source.expectedIsolation());
    }
}
