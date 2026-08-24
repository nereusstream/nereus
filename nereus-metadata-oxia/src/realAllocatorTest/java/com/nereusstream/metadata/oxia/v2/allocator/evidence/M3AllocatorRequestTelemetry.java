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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.registry.allocator.AllocatorEvidenceContextV1;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.EventKind;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.EventOutcome;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.OxiaOperationKind;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.TriggerKind;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Emits only raw NARE1 events. It has no aggregate metric, threshold, or selection input. */
final class M3AllocatorRequestTelemetry {
    private final EventSink sink;
    private final long executionStartNanos;
    private final AtomicLong nextOxiaInvocationToken = new AtomicLong(1);
    private final AtomicLong nextWriteToken = new AtomicLong(1);

    M3AllocatorRequestTelemetry(EventSink sink, long executionStartNanos) {
        this.sink = Objects.requireNonNull(sink, "sink");
        this.executionStartNanos = executionStartNanos;
    }

    RequestTrace trace(
            AllocatorEvidenceContextV1 context,
            M3AllocatorWorkloadPlan.PlannedRequest request,
            AllocatorFaultCutV1 faultCut,
            long ownerEpoch) {
        return new RequestTrace(context, request, faultCut, ownerEpoch);
    }

    @FunctionalInterface
    interface EventSink {
        void append(AllocatorRawEvidenceEventV1 event);
    }

    final class RequestTrace {
        private final AllocatorEvidenceContextV1 context;
        private final long ordinal;
        private final int actorId;
        private final int ledgerIndex;
        private final TriggerKind trigger;
        private final boolean warmup;
        private final AllocatorFaultCutV1 faultCut;
        private final AtomicBoolean admitted = new AtomicBoolean();
        private final AtomicInteger appendAdmissionState = new AtomicInteger();
        private final AtomicInteger nextOperationSequence = new AtomicInteger();
        private volatile long ownerEpoch;

        private RequestTrace(
                AllocatorEvidenceContextV1 context,
                M3AllocatorWorkloadPlan.PlannedRequest request,
                AllocatorFaultCutV1 faultCut,
                long ownerEpoch) {
            this.context = Objects.requireNonNull(context, "context");
            Objects.requireNonNull(request, "request");
            ordinal = request.requestOrdinal();
            actorId = request.actorId();
            ledgerIndex = request.ledgerIndex();
            trigger = switch (request.trigger()) {
                case ENTRY -> TriggerKind.ENTRY;
                case BYTE -> TriggerKind.BYTE;
                case AGE -> TriggerKind.AGE;
            };
            warmup = request.phase() == M3AllocatorWorkloadPlan.Phase.WARM_UP;
            this.faultCut = faultCut;
            setOwnerEpoch(ownerEpoch);
        }

        void offered() {
            endpoint(EventKind.OFFERED, EventOutcome.NONE);
        }

        void offeredAt(long timestampMicros) {
            if (timestampMicros < 0) {
                throw new IllegalArgumentException("allocator offered timestamp cannot be negative");
            }
            append(
                    OxiaOperationKind.NONE,
                    0,
                    EventKind.OFFERED,
                    EventOutcome.NONE,
                    timestampMicros,
                    0,
                    0,
                    0,
                    0);
        }

        void enqueued() {
            endpoint(EventKind.ENQUEUED, EventOutcome.NONE);
        }

        void dispatched() {
            endpoint(EventKind.DISPATCHED, EventOutcome.NONE);
        }

        void enqueuedAtDepth(long depth) {
            long timestampMicros = nowMicros();
            append(
                    OxiaOperationKind.NONE,
                    0,
                    EventKind.ENQUEUED,
                    EventOutcome.NONE,
                    timestampMicros,
                    0,
                    0,
                    0,
                    0);
            append(
                    OxiaOperationKind.NONE,
                    0,
                    EventKind.QUEUE_DEPTH,
                    EventOutcome.NONE,
                    timestampMicros,
                    depth,
                    AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE,
                    0,
                    0);
        }

        void dispatchedAtDepth(long depth) {
            long timestampMicros = nowMicros();
            append(
                    OxiaOperationKind.NONE,
                    0,
                    EventKind.DISPATCHED,
                    EventOutcome.NONE,
                    timestampMicros,
                    0,
                    0,
                    0,
                    0);
            append(
                    OxiaOperationKind.NONE,
                    0,
                    EventKind.QUEUE_DEPTH,
                    EventOutcome.NONE,
                    timestampMicros,
                    depth,
                    AllocatorRawEvidenceEventV1.QUEUE_DEQUEUE,
                    0,
                    0);
        }

        void admitted() {
            if (!admitted.compareAndSet(false, true)) {
                throw new IllegalStateException("allocator request repeats its admitted endpoint");
            }
            endpoint(EventKind.ADMITTED, EventOutcome.NONE);
        }

        void completed() {
            endpoint(EventKind.COMPLETED, EventOutcome.SUCCESS);
        }

        void fenced() {
            endpoint(EventKind.FENCED, EventOutcome.FENCED);
        }

        void failed() {
            endpoint(EventKind.FAILED, EventOutcome.FAILED);
        }

        void timedOut() {
            endpoint(EventKind.TIMED_OUT, EventOutcome.TIMED_OUT);
        }

        void appendAdmissionStart() {
            if (!admitted.get() || !appendAdmissionState.compareAndSet(0, 1)) {
                throw new IllegalStateException("allocator request append admission start is absent or repeated");
            }
            endpoint(EventKind.APPEND_ADMISSION_START, EventOutcome.NONE);
        }

        void appendAdmissionRelease() {
            if (!appendAdmissionState.compareAndSet(1, 2)) {
                throw new IllegalStateException("allocator request append admission release is absent or repeated");
            }
            endpoint(EventKind.APPEND_ADMISSION_RELEASE, EventOutcome.NONE);
        }

        void completeFailureLifecycle() {
            if (admitted.compareAndSet(false, true)) {
                endpoint(EventKind.ADMITTED, EventOutcome.NONE);
            }
            if (appendAdmissionState.compareAndSet(0, 1)) {
                endpoint(EventKind.APPEND_ADMISSION_START, EventOutcome.NONE);
            }
            if (appendAdmissionState.compareAndSet(1, 2)) {
                endpoint(EventKind.APPEND_ADMISSION_RELEASE, EventOutcome.NONE);
            }
        }

        void ownerLossDetected() {
            endpoint(EventKind.OWNER_LOSS_DETECTED, EventOutcome.NONE);
        }

        void freshOwnerAppendComplete() {
            rawWithOwnerEpoch(
                    EventKind.FRESH_OWNER_APPEND_COMPLETE,
                    EventOutcome.SUCCESS,
                    OxiaOperationKind.NONE,
                    0,
                    0,
                    0,
                    0,
                    ownerEpoch);
        }

        void cutBegin() {
            endpoint(EventKind.CUT_BEGIN, EventOutcome.NONE);
        }

        void cutEnd() {
            endpoint(EventKind.CUT_END, EventOutcome.NONE);
        }

        void assertionFailed() {
            endpoint(EventKind.ASSERTION_FAILED, EventOutcome.FAILED);
        }

        void unexpectedError() {
            endpoint(EventKind.UNEXPECTED_ERROR, EventOutcome.FAILED);
        }

        void grantUse(long grantId, long ledgerId) {
            rawWithOwnerEpoch(
                    EventKind.GRANT_USE,
                    EventOutcome.NONE,
                    OxiaOperationKind.NONE,
                    0,
                    grantId,
                    0,
                    ledgerId,
                    ownerEpoch);
        }

        void grantWaste(long grantId, long count) {
            rawWithOwnerEpoch(
                    EventKind.GRANT_WASTE,
                    EventOutcome.NONE,
                    OxiaOperationKind.NONE,
                    0,
                    grantId,
                    count,
                    0,
                    ownerEpoch);
        }

        void staleCandidateBurn(long grantId, long ledgerId) {
            rawWithOwnerEpoch(
                    EventKind.STALE_CANDIDATE_BURN,
                    EventOutcome.NONE,
                    OxiaOperationKind.NONE,
                    0,
                    grantId,
                    0,
                    ledgerId,
                    ownerEpoch);
        }

        void permanentOrphan(long ledgerId) {
            raw(
                    EventKind.PERMANENT_ORPHAN,
                    EventOutcome.NONE,
                    OxiaOperationKind.NONE,
                    0,
                    0,
                    0,
                    ledgerId);
        }

        void allocatedLedgerId(long ledgerId) {
            rawWithOwnerEpoch(
                    EventKind.ALLOCATED_LEDGER_ID,
                    EventOutcome.NONE,
                    OxiaOperationKind.NONE,
                    0,
                    0,
                    0,
                    ledgerId,
                    context.nativePath() ? 0 : ownerEpoch);
        }

        OxiaOperation startOxia(OxiaOperationKind kind, long requestBytes) {
            Objects.requireNonNull(kind, "kind");
            int sequence = nextOperationSequence.getAndIncrement();
            if (sequence > 31) {
                throw new IllegalStateException(
                        "one allocator request dispatched more than thirty-two Oxia operations");
            }
            long invocationToken = nextOxiaInvocationToken.getAndIncrement();
            if (invocationToken <= 0) {
                throw new IllegalStateException("allocator Oxia invocation token overflowed");
            }
            long startMicros = nowMicros();
            append(
                    kind,
                    sequence,
                    EventKind.OXIA_OPERATION_START,
                    EventOutcome.NONE,
                    startMicros,
                    requestBytes,
                    invocationToken,
                    0,
                    0);
            return new OxiaOperation(kind, sequence, startMicros, invocationToken);
        }

        void endOxia(OxiaOperation operation, EventOutcome outcome, long responseBytes) {
            Objects.requireNonNull(operation, "operation");
            append(
                    operation.kind(),
                    operation.sequence(),
                    EventKind.OXIA_OPERATION_END,
                    Objects.requireNonNull(outcome, "outcome"),
                    nowMicros(),
                    responseBytes,
                    operation.invocationToken(),
                    0,
                    0);
        }

        long metadataWriteDispatched(OxiaOperation operation, long canonicalBytes) {
            Objects.requireNonNull(operation, "operation");
            long writeToken = nextWriteToken.getAndIncrement();
            if (writeToken <= 0) {
                throw new IllegalStateException("allocator evidence write token overflowed");
            }
            append(
                    operation.kind(),
                    operation.sequence(),
                    EventKind.METADATA_WRITE_DISPATCH,
                    EventOutcome.NONE,
                    nowMicros(),
                    writeToken,
                    canonicalBytes,
                    0,
                    ownerEpoch);
            return writeToken;
        }

        AllocatorFaultCutV1 faultCut() {
            return faultCut;
        }

        int actorId() {
            return actorId;
        }

        String identity() {
            return context.contextId() + ":" + ordinal + ":" + ledgerIndex;
        }

        void sameKeyReread(OxiaOperation operation, long writeToken, long canonicalBytes) {
            append(
                    operation.kind(),
                    operation.sequence(),
                    EventKind.SAME_KEY_REREAD,
                    EventOutcome.NONE,
                    nowMicros(),
                    writeToken,
                    canonicalBytes,
                    0,
                    ownerEpoch);
        }

        void typedTerminal(
                OxiaOperation operation, long writeToken, long canonicalBytes, EventOutcome exactOutcome) {
            append(
                    operation.kind(),
                    operation.sequence(),
                    EventKind.TYPED_TERMINAL_DISPOSITION,
                    Objects.requireNonNull(exactOutcome, "exactOutcome"),
                    nowMicros(),
                    writeToken,
                    canonicalBytes,
                    0,
                    ownerEpoch);
        }

        void setOwnerEpoch(long ownerEpoch) {
            if (ownerEpoch <= 0) {
                throw new IllegalArgumentException("allocator evidence owner epoch must be positive");
            }
            this.ownerEpoch = ownerEpoch;
        }

        private void endpoint(EventKind kind, EventOutcome outcome) {
            raw(kind, outcome, OxiaOperationKind.NONE, 0, 0, 0, 0);
        }

        private void raw(
                EventKind kind,
                EventOutcome outcome,
                OxiaOperationKind operationKind,
                int sequence,
                long value1,
                long value2,
                long allocatedLedgerId) {
            rawWithOwnerEpoch(
                    kind, outcome, operationKind, sequence, value1, value2, allocatedLedgerId, 0);
        }

        private void rawWithOwnerEpoch(
                EventKind kind,
                EventOutcome outcome,
                OxiaOperationKind operationKind,
                int sequence,
                long value1,
                long value2,
                long allocatedLedgerId,
                long eventOwnerEpoch) {
            append(
                    operationKind,
                    sequence,
                    kind,
                    outcome,
                    nowMicros(),
                    value1,
                    value2,
                    allocatedLedgerId,
                    eventOwnerEpoch);
        }

        private void append(
                OxiaOperationKind operationKind,
                int sequence,
                EventKind kind,
                EventOutcome outcome,
                long timestampMicros,
                long value1,
                long value2,
                long allocatedLedgerId,
                long eventOwnerEpoch) {
            sink.append(new AllocatorRawEvidenceEventV1(
                    context,
                    kind,
                    actorId,
                    trigger,
                    outcome,
                    AllocatorRawEvidenceEventV1.flags(faultCut, operationKind, sequence, warmup),
                    ordinal,
                    ledgerIndex,
                    timestampMicros,
                    value1,
                    value2,
                    allocatedLedgerId,
                    eventOwnerEpoch));
        }
    }

    record OxiaOperation(OxiaOperationKind kind, int sequence, long startMicros, long invocationToken) {}

    long currentTimestampMicros() {
        return nowMicros();
    }

    private long nowMicros() {
        return Math.max(0, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - executionStartNanos));
    }
}
