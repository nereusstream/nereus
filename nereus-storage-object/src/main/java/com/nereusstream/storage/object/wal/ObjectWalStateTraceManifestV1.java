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

import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.BindingState;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.BudgetUsage;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.CallLedger;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.CandidateState;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.DispatchDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Event;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.EventKind;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ExternalCallKind;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.FaultClass;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.InitialState;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Isolation;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LaneSequenceDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LaneState;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LocatorPublication;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.PositionDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Protocol;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ProviderDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ReservationDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.SubjectState;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TraceGroup;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.WriterDisposition;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Strict loader and closed-corpus validator for the canonical C trace manifest. */
public final class ObjectWalStateTraceManifestV1 {
    public static final String RESOURCE_NAME = "com/nereusstream/storage/object/wal/object-wal-state-traces-v1.tsv";

    private static final String HEADER = String.join(
            "\t",
            "traceId",
            "group",
            "protocol",
            "initialState",
            "events",
            "faultClass",
            "outcome",
            "candidates",
            "subjects",
            "bindings",
            "lanes",
            "calls",
            "localCounters",
            "budgets",
            "isolation");

    private ObjectWalStateTraceManifestV1() {}

    public static List<ObjectWalStateTraceV1> loadCanonical() throws IOException {
        InputStream stream =
                ObjectWalStateTraceManifestV1.class.getClassLoader().getResourceAsStream(RESOURCE_NAME);
        if (stream == null) {
            throw new IOException("missing canonical trace manifest: " + RESOURCE_NAME);
        }
        try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            List<ObjectWalStateTraceV1> traces = parse(reader);
            validateClosedCorpus(traces);
            return traces;
        }
    }

    public static List<ObjectWalStateTraceV1> parse(Reader source) throws IOException {
        Objects.requireNonNull(source, "source");
        BufferedReader reader = source instanceof BufferedReader buffered ? buffered : new BufferedReader(source);
        String header = reader.readLine();
        if (!HEADER.equals(header)) {
            throw new IllegalArgumentException("non-canonical trace header");
        }
        List<ObjectWalStateTraceV1> traces = new ArrayList<>();
        String line;
        int lineNumber = 1;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            if (line.isEmpty() || line.charAt(0) == '#') {
                throw new IllegalArgumentException("blank/comment rows are forbidden at line " + lineNumber);
            }
            String[] columns = line.split("\t", -1);
            if (columns.length != 15) {
                throw new IllegalArgumentException(
                        "line " + lineNumber + " has " + columns.length + " columns, expected 15");
            }
            try {
                traces.add(parseRow(columns));
            } catch (RuntimeException exception) {
                throw new IllegalArgumentException("invalid trace manifest line " + lineNumber, exception);
            }
        }
        if (traces.isEmpty()) {
            throw new IllegalArgumentException("trace manifest must not be empty");
        }
        return List.copyOf(traces);
    }

    public static void validateClosedCorpus(List<ObjectWalStateTraceV1> traces) {
        Objects.requireNonNull(traces, "traces");
        if (traces.size() != 50) {
            throw new IllegalArgumentException("closed corpus requires exactly 50 traces, got " + traces.size());
        }
        requireUnique(traces, ObjectWalStateTraceV1::traceId, "traceId");

        Map<Protocol, Long> protocols = counts(traces, ObjectWalStateTraceV1::protocol);
        Map<Protocol, Long> expectedProtocols = Map.of(Protocol.COMMON, 42L, Protocol.KAFKA, 4L, Protocol.PULSAR, 4L);
        if (!protocols.equals(expectedProtocols)) {
            throw new IllegalArgumentException("protocol distribution drifted: " + protocols);
        }

        EnumSet<TerminalOutcome> outcomes = traces.stream()
                .map(ObjectWalStateTraceV1::expectedOutcome)
                .collect(() -> EnumSet.noneOf(TerminalOutcome.class), EnumSet::add, EnumSet::addAll);
        if (!outcomes.equals(EnumSet.allOf(TerminalOutcome.class))) {
            throw new IllegalArgumentException("terminal outcome closure drifted: " + outcomes);
        }

        EnumSet<FaultClass> faults = traces.stream()
                .flatMap(trace -> trace.faultClass().stream())
                .collect(() -> EnumSet.noneOf(FaultClass.class), EnumSet::add, EnumSet::addAll);
        if (!faults.equals(EnumSet.allOf(FaultClass.class))) {
            throw new IllegalArgumentException("fault class closure drifted: " + faults);
        }

        EnumSet<TraceGroup> groups = traces.stream()
                .map(ObjectWalStateTraceV1::group)
                .collect(() -> EnumSet.noneOf(TraceGroup.class), EnumSet::add, EnumSet::addAll);
        if (!groups.equals(EnumSet.allOf(TraceGroup.class))) {
            throw new IllegalArgumentException("six-group closure drifted: " + groups);
        }

        Map<CallProfile, Long> profiles = counts(traces, ObjectWalStateTraceManifestV1::callProfile);
        Map<CallProfile, Long> expectedProfiles = new EnumMap<>(CallProfile.class);
        expectedProfiles.put(CallProfile.E0, 25L);
        expectedProfiles.put(CallProfile.PUT1, 7L);
        expectedProfiles.put(CallProfile.PUT2, 3L);
        expectedProfiles.put(CallProfile.PUT1_GET1, 2L);
        expectedProfiles.put(CallProfile.LIST1, 1L);
        expectedProfiles.put(CallProfile.LIST1_GET1, 4L);
        expectedProfiles.put(CallProfile.LIST2_GET1, 2L);
        expectedProfiles.put(CallProfile.LIST2, 2L);
        expectedProfiles.put(CallProfile.LIST2_PREFIX1, 1L);
        expectedProfiles.put(CallProfile.LIST2_PREFIX2_FRAME2, 1L);
        expectedProfiles.put(CallProfile.LIST1_PREFIX1_FRAME1, 1L);
        expectedProfiles.put(CallProfile.LIST1_PREFIX2_FRAME2, 1L);
        if (!profiles.equals(expectedProfiles)) {
            throw new IllegalArgumentException("external call profile distribution drifted: " + profiles);
        }

        for (ObjectWalStateTraceV1 trace : traces) {
            if (trace.expectedCalls().external(ExternalCallKind.OBJECT_HEAD) != 0) {
                throw new IllegalArgumentException(trace.traceId() + " violates C1 OBJECT_HEAD=0");
            }
            int expectedAckCount = trace.expectedSubjects().stream()
                    .mapToInt(SubjectState::ackCount)
                    .sum();
            if (expectedAckCount != trace.expectedCalls().protocolAcks()) {
                throw new IllegalArgumentException(trace.traceId() + " authors ACK twice inconsistently");
            }
            long faultEvents = trace.events().stream()
                    .filter(event -> event.kind() == EventKind.FAULT)
                    .count();
            if (faultEvents != (trace.faultClass().isPresent() ? 1 : 0)) {
                throw new IllegalArgumentException(trace.traceId() + " fault event/class mismatch");
            }
        }
    }

    public static CallProfile callProfile(ObjectWalStateTraceV1 trace) {
        CallLedger calls = trace.expectedCalls();
        for (ExternalCallKind kind : ExternalCallKind.values()) {
            if (kind != ExternalCallKind.OBJECT_CONDITIONAL_PUT
                    && kind != ExternalCallKind.OBJECT_FULL_GET
                    && kind != ExternalCallKind.OBJECT_PREFIX_RANGE_GET
                    && kind != ExternalCallKind.OBJECT_FRAME_RANGE_GET
                    && kind != ExternalCallKind.OBJECT_LIST_PAGE
                    && calls.external(kind) != 0) {
                throw new IllegalArgumentException(trace.traceId() + " has a call outside the frozen profile: " + kind);
            }
        }
        int put = calls.external(ExternalCallKind.OBJECT_CONDITIONAL_PUT);
        int full = calls.external(ExternalCallKind.OBJECT_FULL_GET);
        int prefix = calls.external(ExternalCallKind.OBJECT_PREFIX_RANGE_GET);
        int frame = calls.external(ExternalCallKind.OBJECT_FRAME_RANGE_GET);
        int list = calls.external(ExternalCallKind.OBJECT_LIST_PAGE);
        for (CallProfile profile : CallProfile.values()) {
            if (profile.matches(put, full, prefix, frame, list)) {
                return profile;
            }
        }
        throw new IllegalArgumentException(trace.traceId() + " has no frozen call profile");
    }

    private static ObjectWalStateTraceV1 parseRow(String[] columns) {
        return new ObjectWalStateTraceV1(
                columns[0],
                TraceGroup.valueOf(columns[1]),
                Protocol.valueOf(columns[2]),
                InitialState.valueOf(columns[3]),
                parseEvents(columns[4]),
                dashOptional(columns[5], FaultClass::valueOf),
                TerminalOutcome.valueOf(columns[6]),
                parseList(columns[7], ObjectWalStateTraceManifestV1::parseCandidate),
                parseList(columns[8], ObjectWalStateTraceManifestV1::parseSubject),
                parseList(columns[9], ObjectWalStateTraceManifestV1::parseBinding),
                parseList(columns[10], ObjectWalStateTraceManifestV1::parseLane),
                parseCalls(columns[11], columns[12]),
                parseBudgets(columns[13]),
                Isolation.valueOf(columns[14]));
    }

    private static List<Event> parseEvents(String value) {
        if (value.isEmpty() || value.equals("-")) {
            throw new IllegalArgumentException("events must be authored explicitly");
        }
        List<Event> events = new ArrayList<>();
        for (String encoded : value.split(";", -1)) {
            String[] fields = encoded.split(":", -1);
            EventKind kind = EventKind.valueOf(fields[0]);
            events.add(new Event(kind, Arrays.asList(fields).subList(1, fields.length)));
        }
        return List.copyOf(events);
    }

    private static CandidateState parseCandidate(String encoded) {
        String[] fields = exactFields(encoded, 5);
        return new CandidateState(
                fields[0],
                Integer.parseInt(fields[1]),
                Long.parseLong(fields[2]),
                DispatchDisposition.valueOf(fields[3]),
                ProviderDisposition.valueOf(fields[4]));
    }

    private static SubjectState parseSubject(String encoded) {
        String[] fields = exactFields(encoded, 8);
        return new SubjectState(
                fields[0],
                fields[1],
                fields[2],
                fields[3],
                PositionDisposition.valueOf(fields[4]),
                ReservationDisposition.valueOf(fields[5]),
                LocatorPublication.valueOf(fields[6]),
                Integer.parseInt(fields[7]));
    }

    private static BindingState parseBinding(String encoded) {
        String[] fields = exactFields(encoded, 3);
        return new BindingState(fields[0], Long.parseLong(fields[1]), WriterDisposition.valueOf(fields[2]));
    }

    private static LaneState parseLane(String encoded) {
        String[] fields = exactFields(encoded, 3);
        return new LaneState(
                Integer.parseInt(fields[0]), LaneSequenceDisposition.valueOf(fields[1]), Long.parseLong(fields[2]));
    }

    private static CallLedger parseCalls(String external, String local) {
        EnumMap<ExternalCallKind, Integer> calls = new EnumMap<>(ExternalCallKind.class);
        if (!external.equals("-")) {
            parseAssignments(external).forEach((key, value) -> calls.put(ExternalCallKind.valueOf(key), value));
        }
        Map<String, Integer> localCounts = parseAssignments(local);
        Set<String> localKeys = Set.of("acks", "publications", "waiters", "spool", "crypto");
        if (!localCounts.keySet().equals(localKeys)) {
            throw new IllegalArgumentException("local counter keys must be exact: " + localCounts.keySet());
        }
        return new CallLedger(
                calls,
                localCounts.get("acks"),
                localCounts.get("publications"),
                localCounts.get("waiters"),
                localCounts.get("spool"),
                localCounts.get("crypto"));
    }

    private static BudgetUsage parseBudgets(String encoded) {
        Map<String, Long> values = parseLongAssignments(encoded);
        Set<String> keys = Set.of(
                "requests",
                "putBytes",
                "readBytes",
                "listPages",
                "listedObjects",
                "listedKeyBytes",
                "decodedUnits",
                "elapsedMillis");
        if (!values.keySet().equals(keys)) {
            throw new IllegalArgumentException("budget keys must be exact: " + values.keySet());
        }
        return new BudgetUsage(
                Math.toIntExact(values.get("requests")),
                values.get("putBytes"),
                values.get("readBytes"),
                Math.toIntExact(values.get("listPages")),
                Math.toIntExact(values.get("listedObjects")),
                values.get("listedKeyBytes"),
                Math.toIntExact(values.get("decodedUnits")),
                values.get("elapsedMillis"));
    }

    private static Map<String, Integer> parseAssignments(String encoded) {
        Map<String, Long> longValues = parseLongAssignments(encoded);
        Map<String, Integer> values = new LinkedHashMap<>();
        longValues.forEach((key, value) -> values.put(key, Math.toIntExact(value)));
        return values;
    }

    private static Map<String, Long> parseLongAssignments(String encoded) {
        if (encoded.isEmpty() || encoded.equals("-")) {
            throw new IllegalArgumentException("assignment map must not be empty");
        }
        Map<String, Long> values = new LinkedHashMap<>();
        for (String assignment : encoded.split(",", -1)) {
            String[] fields = assignment.split("=", -1);
            if (fields.length != 2 || !fields[0].matches("[A-Za-z][A-Za-z0-9_]*")) {
                throw new IllegalArgumentException("invalid assignment: " + assignment);
            }
            long parsed = Long.parseLong(fields[1]);
            if (parsed < 0 || values.putIfAbsent(fields[0], parsed) != null) {
                throw new IllegalArgumentException("negative or duplicate assignment: " + assignment);
            }
        }
        return values;
    }

    private static String[] exactFields(String encoded, int count) {
        String[] fields = encoded.split(":", -1);
        if (fields.length != count || Arrays.stream(fields).anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException("expected " + count + " fields: " + encoded);
        }
        return fields;
    }

    private static <T> List<T> parseList(String encoded, Function<String, T> parser) {
        if (encoded.equals("-")) {
            return List.of();
        }
        if (encoded.isEmpty()) {
            throw new IllegalArgumentException("empty list must use '-'");
        }
        return Arrays.stream(encoded.split(";", -1)).map(parser).toList();
    }

    private static <T> Optional<T> dashOptional(String encoded, Function<String, T> parser) {
        return encoded.equals("-") ? Optional.empty() : Optional.of(parser.apply(encoded));
    }

    private static <T> void requireUnique(List<T> values, Function<T, String> key, String label) {
        Set<String> seen = new HashSet<>();
        for (T value : values) {
            if (!seen.add(key.apply(value))) {
                throw new IllegalArgumentException("duplicate " + label + ": " + key.apply(value));
            }
        }
    }

    private static <T, K> Map<K, Long> counts(List<T> values, Function<T, K> classifier) {
        Map<K, Long> counts = new HashMap<>();
        values.forEach(value -> counts.merge(classifier.apply(value), 1L, Long::sum));
        return counts;
    }

    public enum CallProfile {
        E0(0, 0, 0, 0, 0),
        PUT1(1, 0, 0, 0, 0),
        PUT2(2, 0, 0, 0, 0),
        PUT1_GET1(1, 1, 0, 0, 0),
        LIST1(0, 0, 0, 0, 1),
        LIST1_GET1(0, 1, 0, 0, 1),
        LIST2_GET1(0, 1, 0, 0, 2),
        LIST2(0, 0, 0, 0, 2),
        LIST2_PREFIX1(0, 0, 1, 0, 2),
        LIST2_PREFIX2_FRAME2(0, 0, 2, 2, 2),
        LIST1_PREFIX1_FRAME1(0, 0, 1, 1, 1),
        LIST1_PREFIX2_FRAME2(0, 0, 2, 2, 1);

        private final int put;
        private final int full;
        private final int prefix;
        private final int frame;
        private final int list;

        CallProfile(int put, int full, int prefix, int frame, int list) {
            this.put = put;
            this.full = full;
            this.prefix = prefix;
            this.frame = frame;
            this.list = list;
        }

        private boolean matches(int actualPut, int actualFull, int actualPrefix, int actualFrame, int actualList) {
            return put == actualPut
                    && full == actualFull
                    && prefix == actualPrefix
                    && frame == actualFrame
                    && list == actualList;
        }
    }
}
