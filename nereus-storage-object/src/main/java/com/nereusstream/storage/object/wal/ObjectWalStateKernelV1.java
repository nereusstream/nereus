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

import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.DispatchDisposition.DISPATCHED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.DispatchDisposition.NOT_DISPATCHED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ExternalCallKind.OBJECT_CONDITIONAL_PUT;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ExternalCallKind.OBJECT_FRAME_RANGE_GET;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ExternalCallKind.OBJECT_FULL_GET;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ExternalCallKind.OBJECT_LIST_PAGE;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ExternalCallKind.OBJECT_PREFIX_RANGE_GET;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Isolation.BINDING_LOCAL;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Isolation.LANE_LOCAL;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Isolation.WALRUN_SHARED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LaneSequenceDisposition.ALLOCATED_HELD;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LaneSequenceDisposition.BURNED_WITH_RUN_STOP;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LaneSequenceDisposition.EXHAUSTED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LaneSequenceDisposition.RESOLVED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LocatorPublication.HIDDEN_INSTALLED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LocatorPublication.ROLLED_BACK;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LocatorPublication.VISIBLE_PUBLISHED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.PositionDisposition.ASSIGNED_HELD;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.PositionDisposition.COMMITTED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.PositionDisposition.NOT_ALLOCATED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.PositionDisposition.RESUMED_SAME_POSITION;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.PositionDisposition.ROLLED_BACK_SPECULATIVE_SUFFIX;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.PositionDisposition.SEALED_BEFORE_GAP;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.PositionDisposition.UNKNOWN_RETAINED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ProviderDisposition.APPLIED_EXACT;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ProviderDisposition.DEFINITIVELY_NOT_APPLIED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ProviderDisposition.DEFINITIVE_CONFLICT;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ProviderDisposition.DISPATCHED_UNRESOLVED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ProviderDisposition.EXISTING_EXACT;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ProviderDisposition.NOT_CALLED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ProviderDisposition.QUARANTINED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ReservationDisposition.NONE_ACQUIRED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ReservationDisposition.RELEASED_AFTER_PROVIDER_RESOLUTION;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ReservationDisposition.RELEASED_AFTER_PUBLICATION;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ReservationDisposition.RELEASED_AFTER_ROLLBACK;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ReservationDisposition.RETAINED_PENDING;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ReservationDisposition.TRANSFERRED_TO_RECOVERY;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.BACKPRESSURED_BEFORE_POSITION;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.BINDING_FAILURE_ISOLATED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.BINDING_WRITER_FENCED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.CAPACITY_REJECTED_BEFORE_POSITION;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.FAIL_CLOSED_UNKNOWN;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.LANE_SEQUENCE_EXHAUSTED_SUCCESSOR_REQUIRED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.OBJECT_QUARANTINED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.PLAN_SEALED_NO_EXTERNAL_EFFECT;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.POSITION_ASSIGNMENT_FAILED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.PROVIDER_DEFINITIVE_CONFLICT;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.PROVIDER_RESOLVED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.PUBLICATION_BLOCKED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.PUBLISHED_AND_ACKED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.RESUME_SAME_APPEND_SAME_POSITION;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.RETRY_SAME_PENDING_APPEND;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.ROLLBACK_SPECULATIVE_SUFFIX;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.ROLLOVER_SUCCESSOR_VIRTUAL_LEDGER;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.SAME_CANDIDATE_PUT_RETRY;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.STOP_OLD_WALRUN_BURN_SEQUENCE;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.STOP_WALRUN_SHARED_INVARIANT;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome.TAKEOVER_REBUILT;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.WriterDisposition.ADMITTING;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.WriterDisposition.FENCED;
import static com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.WriterDisposition.STOPPED;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.BindingState;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.BudgetUsage;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.CallLedger;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.CandidateState;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Event;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ExternalCallKind;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.FaultClass;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.Isolation;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LaneSequenceDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LaneState;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.LocatorPublication;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.PositionDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ProviderDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.ReservationDisposition;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.SubjectState;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.TerminalOutcome;
import com.nereusstream.storage.object.wal.ObjectWalStateTraceV1.WriterDisposition;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Deterministic, side-effect-free state kernel for the closed M3 C trace corpus. */
public final class ObjectWalStateKernelV1 {
    private static final long CANONICAL_BODY_BYTES = 1_000;
    private static final long FULL_GET_BYTES = 1_000;
    private static final long PREFIX_GET_BYTES = 200;
    private static final long FRAME_GET_BYTES = 300;
    private static final long LISTED_KEY_BYTES = 64;

    public Result replay(ObjectWalStateTraceV1 trace) {
        Objects.requireNonNull(trace, "trace");
        if (trace.initialState() != ObjectWalStateTraceV1.InitialState.VERIFIED_WALRUN_OPEN) {
            throw new IllegalArgumentException("unsupported initial state: " + trace.initialState());
        }
        MutableState state = new MutableState();
        for (Event event : trace.events()) {
            apply(state, event);
        }
        if (state.outcome == null) {
            throw new IllegalStateException(trace.traceId() + " did not reach a terminal outcome");
        }
        return state.snapshot();
    }

    private static void apply(MutableState state, Event event) {
        switch (event.kind()) {
            case FAULT -> {
                require(state.faultClass == null, "fault class already selected");
                state.faultClass = FaultClass.valueOf(event.argument(0));
            }
            case ADMISSION_REJECT -> state.outcome = CAPACITY_REJECTED_BEFORE_POSITION;
            case BACKPRESSURE -> state.outcome = BACKPRESSURED_BEFORE_POSITION;
            case SUBJECT -> state.addSubject(event);
            case RESERVE -> state.subject(event.argument(0)).reserve();
            case ASSIGN -> state.subject(event.argument(0)).assign();
            case POSITION_FAIL -> {
                MutableSubject subject = state.subject(event.argument(0));
                require(subject.position == NOT_ALLOCATED, "position already allocated");
                require(subject.reservation == RETAINED_PENDING, "position failure requires reservation");
                subject.reservation = RELEASED_AFTER_ROLLBACK;
                state.isolation = BINDING_LOCAL;
                state.outcome = POSITION_ASSIGNMENT_FAILED;
            }
            case PLAN -> state.plan(event);
            case RETRY -> {
                MutableSubject subject = state.subject(event.argument(0));
                require(subject.reservation == RETAINED_PENDING, "retry requires retained reservation");
                state.isolation = BINDING_LOCAL;
                state.outcome = RETRY_SAME_PENDING_APPEND;
            }
            case FENCE -> {
                MutableBinding binding = state.binding(event.argument(0));
                binding.writer = FENCED;
                state.isolation = BINDING_LOCAL;
                state.outcome = BINDING_WRITER_FENCED;
            }
            case RESUME -> {
                MutableSubject subject = state.subject(event.argument(0));
                require(
                        subject.position == ASSIGNED_HELD || subject.position == UNKNOWN_RETAINED,
                        "resume has no position");
                subject.position = RESUMED_SAME_POSITION;
                state.outcome = RESUME_SAME_APPEND_SAME_POSITION;
            }
            case ROLLBACK -> {
                MutableSubject subject = state.subject(event.argument(0));
                require(subject.position != COMMITTED, "cannot roll back a committed subject");
                subject.position = ROLLED_BACK_SPECULATIVE_SUFFIX;
                subject.reservation = RELEASED_AFTER_ROLLBACK;
                subject.locator = ROLLED_BACK;
                if (state.isolation == ObjectWalStateTraceV1.Isolation.NONE) {
                    state.isolation = BINDING_LOCAL;
                }
                state.outcome = ROLLBACK_SPECULATIVE_SUFFIX;
            }
            case ROLLOVER -> {
                MutableSubject subject = state.subject(event.argument(0));
                require(subject.position != COMMITTED, "cannot roll over a committed subject");
                subject.position = SEALED_BEFORE_GAP;
                subject.reservation = TRANSFERRED_TO_RECOVERY;
                state.outcome = ROLLOVER_SUCCESSOR_VIRTUAL_LEDGER;
            }
            case DISPATCH -> state.dispatch(event.argument(0));
            case PUT_APPLIED -> state.put(event.argument(0), APPLIED_EXACT);
            case PUT_NO_EFFECT -> state.put(event.argument(0), DEFINITIVELY_NOT_APPLIED);
            case PUT_LOST_APPLIED, PUT_LOST_NO_EFFECT -> state.put(event.argument(0), DISPATCHED_UNRESOLVED);
            case PUT_CONFLICT -> {
                state.put(event.argument(0), DEFINITIVE_CONFLICT);
                state.outcome = PROVIDER_DEFINITIVE_CONFLICT;
            }
            case FULL_GET_EXACT -> {
                MutableCandidate candidate = state.candidate(event.argument(0));
                require(candidate.dispatch == DISPATCHED, "full GET requires a dispatched candidate");
                state.external(OBJECT_FULL_GET);
                state.budgets.read(FULL_GET_BYTES, 1);
                candidate.provider = EXISTING_EXACT;
            }
            case FULL_GET_MISMATCH -> {
                MutableCandidate candidate = state.candidate(event.argument(0));
                require(candidate.dispatch == DISPATCHED, "full GET requires a dispatched candidate");
                state.external(OBJECT_FULL_GET);
                state.budgets.read(FULL_GET_BYTES, 1);
                candidate.provider = QUARANTINED;
                state.isolation = WALRUN_SHARED;
                state.outcome = OBJECT_QUARANTINED;
            }
            case LIST_EMPTY -> {
                state.external(OBJECT_LIST_PAGE);
                state.budgets.list(0);
            }
            case LIST_ONE -> {
                state.external(OBJECT_LIST_PAGE);
                state.budgets.list(1);
            }
            case PREFIX_GET -> {
                state.candidate(event.argument(0));
                state.external(OBJECT_PREFIX_RANGE_GET);
                state.budgets.read(PREFIX_GET_BYTES, 0);
            }
            case FRAME_GET -> {
                state.subject(event.argument(0));
                state.external(OBJECT_FRAME_RANGE_GET);
                state.budgets.read(FRAME_GET_BYTES, 1);
            }
            case FIXTURE_RESOLVE -> {
                MutableCandidate candidate = state.candidate(event.argument(0));
                candidate.dispatch = DISPATCHED;
                candidate.provider = APPLIED_EXACT;
                state.resolve(candidate);
            }
            case RESOLVE -> {
                state.resolve(state.candidate(event.argument(0)));
                state.outcome = PROVIDER_RESOLVED;
            }
            case ABSENT -> {
                MutableCandidate candidate = state.candidate(event.argument(0));
                candidate.provider = DEFINITIVELY_NOT_APPLIED;
            }
            case UNKNOWN -> {
                MutableCandidate candidate = state.candidate(event.argument(0));
                candidate.provider = DISPATCHED_UNRESOLVED;
                state.subjectsFor(candidate.id).forEach(subject -> subject.position = UNKNOWN_RETAINED);
                state.isolation = LANE_LOCAL;
                state.outcome = FAIL_CLOSED_UNKNOWN;
            }
            case HIDE -> {
                MutableSubject subject = state.subject(event.argument(0));
                require(state.isResolved(state.candidate(subject.candidateId)), "locator requires resolved extent");
                require(
                        subject.reservation == RELEASED_AFTER_PROVIDER_RESOLUTION
                                || subject.reservation == TRANSFERRED_TO_RECOVERY,
                        "locator requires provider resolution or recovery transfer");
                subject.locator = HIDDEN_INSTALLED;
            }
            case PUBLISH -> {
                MutableSubject subject = state.subject(event.argument(0));
                require(subject.locator == HIDDEN_INSTALLED, "publication requires hidden locator");
                require(
                        subject.position == ASSIGNED_HELD || subject.position == RESUMED_SAME_POSITION,
                        "invalid position");
                subject.position = COMMITTED;
                subject.reservation = RELEASED_AFTER_PUBLICATION;
                subject.locator = VISIBLE_PUBLISHED;
                state.binding(subject.bindingId).frontier++;
                state.publications++;
            }
            case ACK -> {
                MutableSubject subject = state.subject(event.argument(0));
                require(
                        subject.position == COMMITTED && subject.locator == VISIBLE_PUBLISHED,
                        "ACK before publication");
                require(subject.acks == 0, "duplicate ACK");
                subject.acks++;
                state.protocolAcks++;
                state.outcome = PUBLISHED_AND_ACKED;
            }
            case BINDING_FAIL -> {
                MutableSubject subject = state.subject(event.argument(0));
                require(subject.position != COMMITTED, "binding failure after publication");
                subject.position = UNKNOWN_RETAINED;
                if (subject.locator == HIDDEN_INSTALLED) {
                    subject.locator = ROLLED_BACK;
                }
                state.isolation = BINDING_LOCAL;
                state.outcome = BINDING_FAILURE_ISOLATED;
            }
            case SHARED_FAIL -> {
                MutableCandidate candidate = state.candidate(event.argument(0));
                candidate.provider = QUARANTINED;
                MutableLane lane = state.lane(candidate.laneId);
                lane.sequence = BURNED_WITH_RUN_STOP;
                state.subjectsFor(candidate.id).forEach(subject -> {
                    subject.position = UNKNOWN_RETAINED;
                    if (subject.locator == HIDDEN_INSTALLED) {
                        subject.locator = ROLLED_BACK;
                    }
                    state.binding(subject.bindingId).writer = STOPPED;
                });
                state.isolation = WALRUN_SHARED;
                state.outcome = STOP_WALRUN_SHARED_INVARIANT;
            }
            case BLOCK -> {
                MutableSubject subject = state.subject(event.argument(0));
                require(subject.position != COMMITTED, "published subject cannot be blocked");
                state.outcome = PUBLICATION_BLOCKED;
            }
            case BURN -> {
                MutableCandidate candidate = state.candidate(event.argument(0));
                state.lane(candidate.laneId).sequence = BURNED_WITH_RUN_STOP;
                state.subjectsFor(candidate.id).forEach(subject -> {
                    subject.position = SEALED_BEFORE_GAP;
                    subject.reservation = TRANSFERRED_TO_RECOVERY;
                    state.binding(subject.bindingId).writer = STOPPED;
                });
                state.isolation = LANE_LOCAL;
                state.outcome = STOP_OLD_WALRUN_BURN_SEQUENCE;
            }
            case STOP_SHARED -> {
                MutableCandidate candidate = state.candidate(event.argument(0));
                state.lane(candidate.laneId).sequence = BURNED_WITH_RUN_STOP;
                state.subjectsFor(candidate.id).forEach(subject -> {
                    subject.position = UNKNOWN_RETAINED;
                    state.binding(subject.bindingId).writer = STOPPED;
                });
                state.isolation = WALRUN_SHARED;
                state.outcome = STOP_WALRUN_SHARED_INVARIANT;
            }
            case EXHAUST -> {
                int laneId = parseLane(event.argument(0));
                MutableLane lane = state.lane(laneId);
                require(lane.lastSequence == Long.MAX_VALUE, "lane exhaustion is injectable only after Long.MAX_VALUE");
                lane.sequence = EXHAUSTED;
                state.isolation = LANE_LOCAL;
                state.outcome = LANE_SEQUENCE_EXHAUSTED_SUCCESSOR_REQUIRED;
            }
            case TRANSFER -> {
                MutableSubject subject = state.subject(event.argument(0));
                subject.reservation = TRANSFERRED_TO_RECOVERY;
            }
            case TAKEOVER -> {
                MutableCandidate candidate = state.candidate(event.argument(0));
                require(
                        candidate.provider == EXISTING_EXACT || candidate.provider == APPLIED_EXACT,
                        "unverified takeover");
                if (!state.isResolved(candidate)) {
                    state.resolve(candidate);
                }
                state.subjectsFor(candidate.id).forEach(subject -> {
                    if (subject.reservation == RETAINED_PENDING) {
                        subject.reservation = TRANSFERRED_TO_RECOVERY;
                    }
                });
                state.outcome = TAKEOVER_REBUILT;
            }
            case SAME_CANDIDATE_RETRY -> {
                MutableCandidate candidate = state.candidate(event.argument(0));
                require(candidate.provider == APPLIED_EXACT, "same-candidate retry did not apply exact");
                state.outcome = SAME_CANDIDATE_PUT_RETRY;
            }
        }
    }

    private static int parseLane(String value) {
        try {
            int laneId = Integer.parseInt(value);
            if (laneId < 0 || laneId > 2) {
                throw new IllegalArgumentException("laneId must be in [0,2]");
            }
            return laneId;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("invalid laneId: " + value, exception);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    public record Result(
            Optional<FaultClass> faultClass,
            TerminalOutcome outcome,
            List<CandidateState> candidates,
            List<SubjectState> subjects,
            List<BindingState> bindings,
            List<LaneState> lanes,
            CallLedger calls,
            BudgetUsage budgets,
            Isolation isolation) {}

    private static final class MutableState {
        private final Map<String, MutableCandidate> candidates = new LinkedHashMap<>();
        private final Map<String, MutableSubject> subjects = new LinkedHashMap<>();
        private final Map<String, MutableBinding> bindings = new LinkedHashMap<>();
        private final Map<Integer, MutableLane> lanes = new LinkedHashMap<>();
        private final EnumMap<ExternalCallKind, Integer> externalCalls = new EnumMap<>(ExternalCallKind.class);
        private final MutableBudget budgets = new MutableBudget();
        private FaultClass faultClass;
        private TerminalOutcome outcome;
        private Isolation isolation = ObjectWalStateTraceV1.Isolation.NONE;
        private int protocolAcks;
        private int publications;

        private void addSubject(Event event) {
            String subjectId = event.argument(0);
            String candidateId = event.argument(1);
            String bindingId = event.argument(2);
            String appendUnitId = event.argument(3);
            require(!subjects.containsKey(subjectId), "duplicate subject: " + subjectId);
            subjects.put(subjectId, new MutableSubject(subjectId, candidateId, bindingId, appendUnitId));
            bindings.computeIfAbsent(bindingId, MutableBinding::new);
        }

        private void plan(Event event) {
            String candidateId = event.argument(0);
            int laneId = parseLane(event.argument(1));
            long laneSequence;
            try {
                laneSequence = Long.parseLong(event.argument(2));
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException("invalid lane sequence: " + event.argument(2), exception);
            }
            require(laneSequence >= 0, "negative lane sequence");
            require(!candidates.containsKey(candidateId), "duplicate candidate: " + candidateId);
            MutableLane lane = lanes.computeIfAbsent(laneId, MutableLane::new);
            require(lane.sequence != ALLOCATED_HELD, "lane already has an unresolved candidate");
            if (lane.lastSequence >= 0) {
                require(lane.lastSequence != Long.MAX_VALUE, "lane sequence overflow");
                require(laneSequence == lane.lastSequence + 1, "lane sequence is not contiguous");
            }
            lane.lastSequence = laneSequence;
            lane.sequence = ALLOCATED_HELD;
            candidates.put(candidateId, new MutableCandidate(candidateId, laneId, laneSequence));
            outcome = PLAN_SEALED_NO_EXTERNAL_EFFECT;
        }

        private void dispatch(String candidateId) {
            MutableCandidate candidate = candidate(candidateId);
            require(candidate.dispatch == NOT_DISPATCHED, "candidate already dispatched");
            candidate.dispatch = DISPATCHED;
            candidate.provider = DISPATCHED_UNRESOLVED;
        }

        private void put(String candidateId, ProviderDisposition result) {
            MutableCandidate candidate = candidate(candidateId);
            require(candidate.dispatch == DISPATCHED, "PUT before dispatch");
            require(candidate.provider == DISPATCHED_UNRESOLVED, "PUT candidate is not unresolved");
            external(OBJECT_CONDITIONAL_PUT);
            budgets.put(CANONICAL_BODY_BYTES);
            candidate.provider = result;
        }

        private void resolve(MutableCandidate candidate) {
            require(
                    candidate.provider == APPLIED_EXACT || candidate.provider == EXISTING_EXACT,
                    "candidate is not exact");
            MutableLane lane = lane(candidate.laneId);
            require(lane.sequence == ALLOCATED_HELD, "lane is not awaiting resolution");
            lane.sequence = RESOLVED;
            lane.frontier++;
            subjectsFor(candidate.id).forEach(subject -> {
                if (subject.reservation == RETAINED_PENDING) {
                    subject.reservation =
                            ObjectWalStateTraceV1.ReservationDisposition.RELEASED_AFTER_PROVIDER_RESOLUTION;
                }
            });
        }

        private boolean isResolved(MutableCandidate candidate) {
            return lane(candidate.laneId).sequence == RESOLVED
                    && (candidate.provider == APPLIED_EXACT || candidate.provider == EXISTING_EXACT);
        }

        private MutableCandidate candidate(String candidateId) {
            MutableCandidate candidate = candidates.get(candidateId);
            if (candidate == null) {
                throw new IllegalStateException("unknown candidate: " + candidateId);
            }
            return candidate;
        }

        private MutableSubject subject(String subjectId) {
            MutableSubject subject = subjects.get(subjectId);
            if (subject == null) {
                throw new IllegalStateException("unknown subject: " + subjectId);
            }
            return subject;
        }

        private MutableBinding binding(String bindingId) {
            MutableBinding binding = bindings.get(bindingId);
            if (binding == null) {
                throw new IllegalStateException("unknown binding: " + bindingId);
            }
            return binding;
        }

        private MutableLane lane(int laneId) {
            MutableLane lane = lanes.get(laneId);
            if (lane == null) {
                throw new IllegalStateException("unknown lane: " + laneId);
            }
            return lane;
        }

        private List<MutableSubject> subjectsFor(String candidateId) {
            return subjects.values().stream()
                    .filter(subject -> subject.candidateId.equals(candidateId))
                    .toList();
        }

        private void external(ExternalCallKind kind) {
            externalCalls.merge(kind, 1, Integer::sum);
        }

        private Result snapshot() {
            List<CandidateState> candidateStates =
                    candidates.values().stream().map(MutableCandidate::snapshot).toList();
            List<SubjectState> subjectStates =
                    subjects.values().stream().map(MutableSubject::snapshot).toList();
            List<BindingState> bindingStates =
                    bindings.values().stream().map(MutableBinding::snapshot).toList();
            List<LaneState> laneStates =
                    lanes.values().stream().map(MutableLane::snapshot).toList();
            CallLedger calls = new CallLedger(externalCalls, protocolAcks, publications, 0, 0, 0);
            int derivedAcks =
                    subjectStates.stream().mapToInt(SubjectState::ackCount).sum();
            require(derivedAcks == protocolAcks, "ACK ledger differs from subject states");
            return new Result(
                    Optional.ofNullable(faultClass),
                    outcome,
                    candidateStates,
                    subjectStates,
                    bindingStates,
                    laneStates,
                    calls,
                    budgets.snapshot(),
                    isolation);
        }
    }

    private static final class MutableCandidate {
        private final String id;
        private final int laneId;
        private final long laneSequence;
        private ObjectWalStateTraceV1.DispatchDisposition dispatch = NOT_DISPATCHED;
        private ProviderDisposition provider = NOT_CALLED;

        private MutableCandidate(String id, int laneId, long laneSequence) {
            this.id = id;
            this.laneId = laneId;
            this.laneSequence = laneSequence;
        }

        private CandidateState snapshot() {
            return new CandidateState(id, laneId, laneSequence, dispatch, provider);
        }
    }

    private static final class MutableSubject {
        private final String id;
        private final String candidateId;
        private final String bindingId;
        private final String appendUnitId;
        private PositionDisposition position = NOT_ALLOCATED;
        private ReservationDisposition reservation = NONE_ACQUIRED;
        private LocatorPublication locator = ObjectWalStateTraceV1.LocatorPublication.NONE;
        private int acks;

        private MutableSubject(String id, String candidateId, String bindingId, String appendUnitId) {
            this.id = id;
            this.candidateId = candidateId;
            this.bindingId = bindingId;
            this.appendUnitId = appendUnitId;
        }

        private void reserve() {
            require(reservation == NONE_ACQUIRED, "reservation already acquired");
            require(position == NOT_ALLOCATED, "reservation must precede position allocation");
            reservation = RETAINED_PENDING;
        }

        private void assign() {
            require(reservation == RETAINED_PENDING, "position allocation requires reservation");
            require(position == NOT_ALLOCATED, "position already assigned");
            position = ASSIGNED_HELD;
        }

        private SubjectState snapshot() {
            return new SubjectState(id, candidateId, bindingId, appendUnitId, position, reservation, locator, acks);
        }
    }

    private static final class MutableBinding {
        private final String id;
        private long frontier;
        private WriterDisposition writer = ADMITTING;

        private MutableBinding(String id) {
            this.id = id;
        }

        private BindingState snapshot() {
            return new BindingState(id, frontier, writer);
        }
    }

    private static final class MutableLane {
        private final int id;
        private long lastSequence = -1;
        private LaneSequenceDisposition sequence = ObjectWalStateTraceV1.LaneSequenceDisposition.NOT_ALLOCATED;
        private long frontier;

        private MutableLane(int id) {
            this.id = id;
        }

        private LaneState snapshot() {
            return new LaneState(id, sequence, frontier);
        }
    }

    private static final class MutableBudget {
        private int providerRequests;
        private long putBytes;
        private long readBytes;
        private int listPages;
        private int listedObjects;
        private long listedKeyBytes;
        private int decodedUnits;
        private long elapsedMillis;

        private void put(long bytes) {
            providerRequests++;
            putBytes += bytes;
            elapsedMillis++;
        }

        private void read(long bytes, int decoded) {
            providerRequests++;
            readBytes += bytes;
            decodedUnits += decoded;
            elapsedMillis++;
        }

        private void list(int objects) {
            providerRequests++;
            listPages++;
            listedObjects += objects;
            listedKeyBytes += objects * LISTED_KEY_BYTES;
            elapsedMillis++;
        }

        private BudgetUsage snapshot() {
            return new BudgetUsage(
                    providerRequests,
                    putBytes,
                    readBytes,
                    listPages,
                    listedObjects,
                    listedKeyBytes,
                    decodedUnits,
                    elapsedMillis);
        }
    }
}
