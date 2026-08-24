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

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One completely authored OBJECT_WAL_STATE_TRACE_V1 corpus entry. */
public record ObjectWalStateTraceV1(
        String traceId,
        TraceGroup group,
        Protocol protocol,
        InitialState initialState,
        List<Event> events,
        Optional<FaultClass> faultClass,
        TerminalOutcome expectedOutcome,
        List<CandidateState> expectedCandidates,
        List<SubjectState> expectedSubjects,
        List<BindingState> expectedBindings,
        List<LaneState> expectedLanes,
        CallLedger expectedCalls,
        BudgetUsage expectedBudgets,
        Isolation expectedIsolation) {

    public ObjectWalStateTraceV1 {
        traceId = requireIdentifier(traceId, "traceId");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(protocol, "protocol");
        Objects.requireNonNull(initialState, "initialState");
        events = List.copyOf(events);
        if (events.isEmpty()) {
            throw new IllegalArgumentException("events must not be empty");
        }
        faultClass = Objects.requireNonNull(faultClass, "faultClass");
        Objects.requireNonNull(expectedOutcome, "expectedOutcome");
        expectedCandidates = uniqueCandidates(expectedCandidates);
        expectedSubjects = uniqueSubjects(expectedSubjects);
        expectedBindings = uniqueBindings(expectedBindings);
        expectedLanes = uniqueLanes(expectedLanes);
        Objects.requireNonNull(expectedCalls, "expectedCalls");
        Objects.requireNonNull(expectedBudgets, "expectedBudgets");
        Objects.requireNonNull(expectedIsolation, "expectedIsolation");
    }

    private static List<CandidateState> uniqueCandidates(List<CandidateState> values) {
        List<CandidateState> copy = List.copyOf(values);
        if (copy.stream().map(CandidateState::candidateId).distinct().count() != copy.size()) {
            throw new IllegalArgumentException("duplicate expected candidateId");
        }
        return copy;
    }

    private static List<SubjectState> uniqueSubjects(List<SubjectState> values) {
        List<SubjectState> copy = List.copyOf(values);
        if (copy.stream().map(SubjectState::subjectId).distinct().count() != copy.size()) {
            throw new IllegalArgumentException("duplicate expected subjectId");
        }
        return copy;
    }

    private static List<BindingState> uniqueBindings(List<BindingState> values) {
        List<BindingState> copy = List.copyOf(values);
        if (copy.stream().map(BindingState::bindingId).distinct().count() != copy.size()) {
            throw new IllegalArgumentException("duplicate expected bindingId");
        }
        return copy;
    }

    private static List<LaneState> uniqueLanes(List<LaneState> values) {
        List<LaneState> copy = List.copyOf(values);
        if (copy.stream().map(LaneState::laneId).distinct().count() != copy.size()) {
            throw new IllegalArgumentException("duplicate expected laneId");
        }
        return copy;
    }

    static String requireIdentifier(String value, String label) {
        Objects.requireNonNull(value, label);
        if (!value.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,95}")) {
            throw new IllegalArgumentException(label + " is not a canonical identifier: " + value);
        }
        return value;
    }

    public enum TraceGroup {
        ADMISSION,
        SEQUENCING,
        PROVIDER,
        PUBLICATION,
        RECOVERY,
        ISOLATION
    }

    public enum Protocol {
        COMMON,
        KAFKA,
        PULSAR
    }

    /** C traces start after the immutable Root and crypto context have been verified. */
    public enum InitialState {
        VERIFIED_WALRUN_OPEN
    }

    public enum FaultClass {
        ADMISSION_REJECTION,
        RETRYABLE_BINDING_LOCAL,
        FENCE_BINDING,
        STOP_WALRUN_SHARED_INVARIANT,
        PROVIDER_DEFINITIVE_FAILURE,
        PROVIDER_OUTCOME_UNKNOWN,
        TAKEOVER_RECOVERY
    }

    public enum TerminalOutcome {
        CAPACITY_REJECTED_BEFORE_POSITION,
        BACKPRESSURED_BEFORE_POSITION,
        POSITION_ASSIGNMENT_FAILED,
        PLAN_SEALED_NO_EXTERNAL_EFFECT,
        RETRY_SAME_PENDING_APPEND,
        BINDING_WRITER_FENCED,
        RESUME_SAME_APPEND_SAME_POSITION,
        ROLLBACK_SPECULATIVE_SUFFIX,
        ROLLOVER_SUCCESSOR_VIRTUAL_LEDGER,
        SAME_CANDIDATE_PUT_RETRY,
        PROVIDER_RESOLVED,
        PROVIDER_DEFINITIVE_CONFLICT,
        OBJECT_QUARANTINED,
        FAIL_CLOSED_UNKNOWN,
        STOP_WALRUN_SHARED_INVARIANT,
        STOP_OLD_WALRUN_BURN_SEQUENCE,
        LANE_SEQUENCE_EXHAUSTED_SUCCESSOR_REQUIRED,
        BINDING_FAILURE_ISOLATED,
        PUBLICATION_BLOCKED,
        PUBLISHED_AND_ACKED,
        TAKEOVER_REBUILT
    }

    public enum DispatchDisposition {
        NOT_DISPATCHED,
        DISPATCHED
    }

    public enum PositionDisposition {
        NOT_ALLOCATED,
        ASSIGNED_HELD,
        RESUMED_SAME_POSITION,
        ROLLED_BACK_SPECULATIVE_SUFFIX,
        SEALED_BEFORE_GAP,
        COMMITTED,
        UNKNOWN_RETAINED
    }

    public enum LaneSequenceDisposition {
        NOT_ALLOCATED,
        ALLOCATED_HELD,
        RESOLVED,
        BURNED_WITH_RUN_STOP,
        EXHAUSTED
    }

    public enum ReservationDisposition {
        NONE_ACQUIRED,
        RETAINED_PENDING,
        TRANSFERRED_TO_RECOVERY,
        RELEASED_AFTER_ROLLBACK,
        RELEASED_AFTER_PROVIDER_RESOLUTION,
        RELEASED_AFTER_PUBLICATION
    }

    public enum ProviderDisposition {
        NOT_CALLED,
        DISPATCHED_UNRESOLVED,
        APPLIED_EXACT,
        EXISTING_EXACT,
        DEFINITIVELY_NOT_APPLIED,
        DEFINITIVE_CONFLICT,
        QUARANTINED
    }

    public enum LocatorPublication {
        NONE,
        HIDDEN_INSTALLED,
        VISIBLE_PUBLISHED,
        ROLLED_BACK
    }

    public enum WriterDisposition {
        ADMITTING,
        FENCED,
        STOPPED
    }

    public enum Isolation {
        NONE,
        BINDING_LOCAL,
        LANE_LOCAL,
        WALRUN_SHARED
    }

    public enum ExternalCallKind {
        ROOT_AUTHORITY_READ,
        METADATA_READ,
        METADATA_CONDITIONAL_MUTATION,
        KMS_WRAP,
        KMS_UNWRAP,
        OBJECT_CONDITIONAL_PUT,
        OBJECT_HEAD,
        OBJECT_FULL_GET,
        OBJECT_PREFIX_RANGE_GET,
        OBJECT_FRAME_RANGE_GET,
        OBJECT_LIST_PAGE
    }

    public enum EventKind {
        FAULT(1),
        ADMISSION_REJECT(0),
        BACKPRESSURE(0),
        SUBJECT(4),
        RESERVE(1),
        ASSIGN(1),
        POSITION_FAIL(1),
        PLAN(3),
        RETRY(1),
        FENCE(1),
        RESUME(1),
        ROLLBACK(1),
        ROLLOVER(1),
        DISPATCH(1),
        PUT_APPLIED(1),
        PUT_NO_EFFECT(1),
        PUT_LOST_APPLIED(1),
        PUT_LOST_NO_EFFECT(1),
        PUT_CONFLICT(1),
        FULL_GET_EXACT(1),
        FULL_GET_MISMATCH(1),
        LIST_EMPTY(0),
        LIST_ONE(0),
        PREFIX_GET(1),
        FRAME_GET(1),
        FIXTURE_RESOLVE(1),
        RESOLVE(1),
        ABSENT(1),
        UNKNOWN(1),
        HIDE(1),
        PUBLISH(1),
        ACK(1),
        BINDING_FAIL(1),
        SHARED_FAIL(1),
        BLOCK(1),
        BURN(1),
        STOP_SHARED(1),
        EXHAUST(1),
        TRANSFER(1),
        TAKEOVER(1),
        SAME_CANDIDATE_RETRY(1);

        private final int argumentCount;

        EventKind(int argumentCount) {
            this.argumentCount = argumentCount;
        }

        public int argumentCount() {
            return argumentCount;
        }
    }

    public record Event(EventKind kind, List<String> arguments) {
        public Event {
            Objects.requireNonNull(kind, "kind");
            arguments = List.copyOf(arguments);
            if (arguments.size() != kind.argumentCount()) {
                throw new IllegalArgumentException(
                        kind + " requires " + kind.argumentCount() + " arguments, got " + arguments.size());
            }
            arguments.forEach(argument -> requireIdentifier(argument, "event argument"));
        }

        public String argument(int index) {
            return arguments.get(index);
        }
    }

    public record CandidateState(
            String candidateId,
            int laneId,
            long laneSequence,
            DispatchDisposition dispatchDisposition,
            ProviderDisposition providerDisposition) {
        public CandidateState {
            candidateId = requireIdentifier(candidateId, "candidateId");
            requireLane(laneId);
            if (laneSequence < 0) {
                throw new IllegalArgumentException("laneSequence must be non-negative");
            }
            Objects.requireNonNull(dispatchDisposition, "dispatchDisposition");
            Objects.requireNonNull(providerDisposition, "providerDisposition");
        }
    }

    public record SubjectState(
            String subjectId,
            String candidateId,
            String bindingId,
            String appendUnitId,
            PositionDisposition positionDisposition,
            ReservationDisposition reservationDisposition,
            LocatorPublication locatorPublication,
            int ackCount) {
        public SubjectState {
            subjectId = requireIdentifier(subjectId, "subjectId");
            candidateId = requireIdentifier(candidateId, "candidateId");
            bindingId = requireIdentifier(bindingId, "bindingId");
            appendUnitId = requireIdentifier(appendUnitId, "appendUnitId");
            Objects.requireNonNull(positionDisposition, "positionDisposition");
            Objects.requireNonNull(reservationDisposition, "reservationDisposition");
            Objects.requireNonNull(locatorPublication, "locatorPublication");
            if (ackCount < 0 || ackCount > 1) {
                throw new IllegalArgumentException("ackCount must be zero or one");
            }
        }
    }

    public record BindingState(String bindingId, long bindingFrontierDelta, WriterDisposition writerDisposition) {
        public BindingState {
            bindingId = requireIdentifier(bindingId, "bindingId");
            if (bindingFrontierDelta < 0) {
                throw new IllegalArgumentException("bindingFrontierDelta must be non-negative");
            }
            Objects.requireNonNull(writerDisposition, "writerDisposition");
        }
    }

    public record LaneState(int laneId, LaneSequenceDisposition laneSequenceDisposition, long laneFrontierDelta) {
        public LaneState {
            requireLane(laneId);
            Objects.requireNonNull(laneSequenceDisposition, "laneSequenceDisposition");
            if (laneFrontierDelta < 0) {
                throw new IllegalArgumentException("laneFrontierDelta must be non-negative");
            }
        }
    }

    public record CallLedger(
            Map<ExternalCallKind, Integer> externalCalls,
            int protocolAcks,
            int publications,
            int waiters,
            int localSpoolOperations,
            int localCryptoOperations) {
        public CallLedger {
            Objects.requireNonNull(externalCalls, "externalCalls");
            EnumMap<ExternalCallKind, Integer> closed = new EnumMap<>(ExternalCallKind.class);
            for (ExternalCallKind kind : ExternalCallKind.values()) {
                int count = externalCalls.getOrDefault(kind, 0);
                if (count < 0) {
                    throw new IllegalArgumentException("negative call count for " + kind);
                }
                closed.put(kind, count);
            }
            if (externalCalls.keySet().stream().anyMatch(key -> !closed.containsKey(key))) {
                throw new IllegalArgumentException("unknown external call kind");
            }
            externalCalls = Map.copyOf(closed);
            requireNonNegative(protocolAcks, "protocolAcks");
            requireNonNegative(publications, "publications");
            requireNonNegative(waiters, "waiters");
            requireNonNegative(localSpoolOperations, "localSpoolOperations");
            requireNonNegative(localCryptoOperations, "localCryptoOperations");
        }

        public int external(ExternalCallKind kind) {
            return externalCalls.get(kind);
        }

        public int totalExternalCalls() {
            return externalCalls.values().stream().mapToInt(Integer::intValue).sum();
        }
    }

    public record BudgetUsage(
            int providerRequests,
            long putBytes,
            long readBytes,
            int listPages,
            int listedObjects,
            long listedKeyBytes,
            int decodedUnits,
            long elapsedMillis) {
        public BudgetUsage {
            requireNonNegative(providerRequests, "providerRequests");
            requireNonNegative(putBytes, "putBytes");
            requireNonNegative(readBytes, "readBytes");
            requireNonNegative(listPages, "listPages");
            requireNonNegative(listedObjects, "listedObjects");
            requireNonNegative(listedKeyBytes, "listedKeyBytes");
            requireNonNegative(decodedUnits, "decodedUnits");
            requireNonNegative(elapsedMillis, "elapsedMillis");
        }
    }

    private static void requireLane(int laneId) {
        if (laneId < 0 || laneId > 2) {
            throw new IllegalArgumentException("laneId must be in [0,2]: " + laneId);
        }
    }

    private static void requireNonNegative(long value, String label) {
        if (value < 0) {
            throw new IllegalArgumentException(label + " must be non-negative");
        }
    }
}
