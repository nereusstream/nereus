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
import com.nereusstream.storage.object.wal.ObjectWalStateTraceManifestV1.CallProfile;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ExternalCallKind;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.FaultClass;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Protocol;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TraceGroup;
import java.io.StringReader;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ObjectWalStateTraceManifestV1Test {
    @Test
    void loadsExplicitClosedFiftyTraceCorpus() throws Exception {
        List<ObjectWalStateTraceV1> traces = ObjectWalStateTraceManifestV1.loadCanonical();

        assertThat(traces).hasSize(50);
        assertThat(traces).extracting(ObjectWalStateTraceV1::traceId).doesNotHaveDuplicates();
        assertThat(traces.stream()
                        .collect(Collectors.groupingBy(ObjectWalStateTraceV1::protocol, Collectors.counting())))
                .isEqualTo(Map.of(Protocol.COMMON, 42L, Protocol.KAFKA, 4L, Protocol.PULSAR, 4L));
        assertThat(traces.stream().map(ObjectWalStateTraceV1::group).collect(Collectors.toSet()))
                .containsExactlyInAnyOrderElementsOf(EnumSet.allOf(TraceGroup.class));
        assertThat(traces.stream().flatMap(trace -> trace.faultClass().stream()).collect(Collectors.toSet()))
                .containsExactlyInAnyOrderElementsOf(EnumSet.allOf(FaultClass.class));
    }

    @Test
    void coversExactTwentyOneOutcomesAndCallProfiles() throws Exception {
        List<ObjectWalStateTraceV1> traces = ObjectWalStateTraceManifestV1.loadCanonical();

        assertThat(traces.stream().map(ObjectWalStateTraceV1::expectedOutcome).collect(Collectors.toSet()))
                .containsExactlyInAnyOrderElementsOf(EnumSet.allOf(TerminalOutcome.class));
        assertThat(traces.stream()
                        .collect(Collectors.groupingBy(
                                ObjectWalStateTraceManifestV1::callProfile, Collectors.counting())))
                .isEqualTo(Map.ofEntries(
                        Map.entry(CallProfile.E0, 25L),
                        Map.entry(CallProfile.PUT1, 7L),
                        Map.entry(CallProfile.PUT2, 3L),
                        Map.entry(CallProfile.PUT1_GET1, 2L),
                        Map.entry(CallProfile.LIST1, 1L),
                        Map.entry(CallProfile.LIST1_GET1, 4L),
                        Map.entry(CallProfile.LIST2_GET1, 2L),
                        Map.entry(CallProfile.LIST2, 2L),
                        Map.entry(CallProfile.LIST2_PREFIX1, 1L),
                        Map.entry(CallProfile.LIST2_PREFIX2_FRAME2, 1L),
                        Map.entry(CallProfile.LIST1_PREFIX1_FRAME1, 1L),
                        Map.entry(CallProfile.LIST1_PREFIX2_FRAME2, 1L)));
        assertThat(traces).allSatisfy(trace -> assertThat(trace.expectedCalls().external(ExternalCallKind.OBJECT_HEAD))
                .as(trace.traceId())
                .isZero());
    }

    @Test
    void authorsEveryExpectationWithoutDuplicateAckAuthority() throws Exception {
        List<ObjectWalStateTraceV1> traces = ObjectWalStateTraceManifestV1.loadCanonical();

        assertThat(traces).allSatisfy(trace -> {
            assertThat(trace.events()).as(trace.traceId()).isNotEmpty();
            assertThat(trace.expectedSubjects().stream()
                            .mapToInt(ObjectWalStateTraceV1.SubjectState::ackCount)
                            .sum())
                    .as(trace.traceId())
                    .isEqualTo(trace.expectedCalls().protocolAcks());
            assertThat(trace.expectedBudgets().providerRequests())
                    .as(trace.traceId())
                    .isEqualTo(trace.expectedCalls().external(ExternalCallKind.OBJECT_CONDITIONAL_PUT)
                            + trace.expectedCalls().external(ExternalCallKind.OBJECT_FULL_GET)
                            + trace.expectedCalls().external(ExternalCallKind.OBJECT_PREFIX_RANGE_GET)
                            + trace.expectedCalls().external(ExternalCallKind.OBJECT_FRAME_RANGE_GET)
                            + trace.expectedCalls().external(ExternalCallKind.OBJECT_LIST_PAGE));
        });
    }

    @Test
    void rejectsPlaceholderRowsAndRuntimeExpansion() {
        assertThatThrownBy(() -> ObjectWalStateTraceManifestV1.parse(new StringReader(
                        "traceId\tgroup\tprotocol\tinitialState\tevents\tfaultClass\toutcome\tcandidates\t"
                                + "subjects\tbindings\tlanes\tcalls\tlocalCounters\tbudgets\tisolation\n"
                                + "BAD_ROW\tADMISSION\tCOMMON\tVERIFIED_WALRUN_OPEN\t-\t-\t"
                                + "CAPACITY_REJECTED_BEFORE_POSITION\t-\t-\t-\t-\t-\t-\t-\tNONE\n")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ObjectWalStateTraceManifestV1.validateClosedCorpus(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly 50");
    }
}
