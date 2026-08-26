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

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.EventKind;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.EventOutcome;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.OxiaOperationKind;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Streaming, caller-aggregate-free ADR-0094 raw validator and closed candidate selector. */
final class AllocatorRawEvidenceValidatorV1 {
    private static final byte[] EVENT_MAGIC = "NARE".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] JUNIT_MAGIC = "NAJT".getBytes(StandardCharsets.US_ASCII);
    private static final int EVENT_HEADER_BYTES = 32;
    private static final int WARMUP_SECONDS = 10;
    private static final int MEASURED_SECONDS = 30;
    private static final long MICROS_PER_MILLI = 1_000;
    private static final int OPERATION_SEQUENCE_BITS = 5;

    private AllocatorRawEvidenceValidatorV1() {}

    static SelectionComputation validate(List<AllocatorEvidenceAttachmentV1> attachments) {
        EnumMap<AllocatorEvidenceAttachmentKindV1, AllocatorEvidenceAttachmentV1> inventory =
                new EnumMap<>(AllocatorEvidenceAttachmentKindV1.class);
        for (AllocatorEvidenceAttachmentV1 attachment : attachments) {
            if (inventory.putIfAbsent(attachment.kind(), attachment) != null) {
                throw invalid("allocator raw evidence attachment kind is duplicated");
            }
        }
        if (inventory.size() != AllocatorEvidenceAttachmentKindV1.values().length) {
            throw invalid("allocator raw evidence attachment inventory is incomplete");
        }
        AllocatorJUnitEvidenceV1.Counts junit = validateJUnit(inventory.get(AllocatorEvidenceAttachmentKindV1.TEST));
        Map<Integer, IntervalAccumulator> intervals = new HashMap<>();
        LongOwnerTable allocatedIds = new LongOwnerTable();
        readEvents(inventory.get(AllocatorEvidenceAttachmentKindV1.NATIVE), intervals, null, allocatedIds);
        readEvents(inventory.get(AllocatorEvidenceAttachmentKindV1.SCALE_10K), intervals, null, allocatedIds);
        readEvents(inventory.get(AllocatorEvidenceAttachmentKindV1.SCALE_100K), intervals, null, allocatedIds);
        FaultMatrix faults = new FaultMatrix();
        readEvents(inventory.get(AllocatorEvidenceAttachmentKindV1.FAULT), null, faults, null);
        if (intervals.size() != 288) {
            throw invalid("allocator raw evidence does not contain all 48 native and 240 candidate intervals");
        }
        Map<Integer, IntervalSummary> summaries = new HashMap<>();
        for (IntervalAccumulator interval : intervals.values()) {
            IntervalSummary summary = interval.finish();
            summaries.put(interval.context.contextId(), summary);
        }
        intervals.clear();
        allocatedIds.release();
        Map<RowKey, FaultRowSummary> faultRows = faults.finish();

        Map<RowKey, NativeRow> nativeRows = new HashMap<>();
        for (int population : AllocatorEvidenceContextV1.POPULATIONS) {
            for (int latency : AllocatorEvidenceContextV1.LATENCIES_MILLIS) {
                IntervalSummary sustainable = null;
                for (int rate : AllocatorEvidenceContextV1.OFFERED_RATES) {
                    IntervalSummary candidate =
                            summaries.get(AllocatorEvidenceContextV1.nativeContext(population, latency, rate)
                                    .contextId());
                    if (candidate.completeZeroFailure) {
                        sustainable = candidate;
                    }
                }
                if (sustainable == null) {
                    throw invalid("native raw evidence has no complete zero-failure offered-rate interval");
                }
                nativeRows.put(
                        RowKey.nativeRow(population, latency),
                        new NativeRow(sustainable.context.offeredRolloverRequestsPerSecond(), sustainable.appendP99));
            }
        }

        List<CandidateQualification> qualified = new ArrayList<>();
        List<AllocatorEvidenceCandidateV1> candidates = new ArrayList<>();
        candidates.add(AllocatorEvidenceCandidateV1.strict());
        AllocatorEvidenceCandidateV1.RANGE_SIZES.forEach(
                size -> candidates.add(AllocatorEvidenceCandidateV1.range(size)));
        for (AllocatorEvidenceCandidateV1 candidate : candidates) {
            List<AllocatorNativeRelativeMetricsV1> rows = new ArrayList<>();
            boolean allRows = junit.zeroFailureErrorSkip();
            for (int population : AllocatorEvidenceContextV1.POPULATIONS) {
                for (int latency : AllocatorEvidenceContextV1.LATENCIES_MILLIS) {
                    IntervalSummary sustainable = null;
                    for (int rate : AllocatorEvidenceContextV1.OFFERED_RATES) {
                        AllocatorEvidenceContextV1 context =
                                AllocatorEvidenceContextV1.candidateContext(candidate, population, latency, rate);
                        IntervalSummary current = summaries.get(context.contextId());
                        if (current.absoluteBoundsPass()) {
                            sustainable = current;
                        }
                    }
                    NativeRow nativeRow = nativeRows.get(RowKey.nativeRow(population, latency));
                    FaultRowSummary fault = faultRows.get(RowKey.candidateRow(candidate, population, latency));
                    boolean rowPass = sustainable != null
                            && sustainable.context.offeredRolloverRequestsPerSecond() >= 200
                            && sustainable.context.offeredRolloverRequestsPerSecond()
                                            / (double) nativeRow.sustainableRate
                                    >= 0.80
                            && sustainable.appendP99 <= nativeRow.appendP99 + 250 * MICROS_PER_MILLI
                            && fault != null
                            && fault.qualifies(population);
                    if (!rowPass) {
                        allRows = false;
                        continue;
                    }
                    rows.add(sustainable.toMetrics(nativeRow, fault));
                }
            }
            if (allRows && rows.size() == 8) {
                qualified.add(new CandidateQualification(candidate, List.copyOf(rows)));
            }
        }
        CandidateQualification strict = qualified.stream()
                .filter(value -> value.candidate.mode() == AllocatorModeV1.STRICT_SERIALIZED)
                .findFirst()
                .orElse(null);
        CandidateQualification range = qualified.stream()
                .filter(value -> value.candidate.mode() == AllocatorModeV1.RANGE_LEASED)
                .min(Comparator.comparingLong(value -> value.candidate.rangeSize()))
                .orElse(null);
        SelectionStatus status;
        CandidateQualification selected = null;
        if (strict != null && range == null) {
            status = SelectionStatus.STRICT_SELECTED;
            selected = strict;
        } else if (strict == null && range != null) {
            status = SelectionStatus.RANGE_SELECTED;
            selected = range;
        } else if (strict == null) {
            status = SelectionStatus.NONE_QUALIFIED;
        } else {
            status = SelectionStatus.BOTH_QUALIFIED;
        }
        return new SelectionComputation(
                status,
                Optional.ofNullable(selected).map(value -> value.candidate),
                selected == null ? List.of() : selected.rows,
                qualified.stream().map(value -> value.candidate).toList(),
                junit);
    }

    private static AllocatorJUnitEvidenceV1.Counts validateJUnit(AllocatorEvidenceAttachmentV1 attachment) {
        try (DataInputStream input = new DataInputStream(attachment.openPayload())) {
            byte[] magic = input.readNBytes(4);
            int schema = input.readUnsignedShort();
            int flags = input.readUnsignedShort();
            long tests = input.readLong();
            long failures = input.readLong();
            long errors = input.readLong();
            long skipped = input.readLong();
            int reportLength = input.readInt();
            byte[] reserved = input.readNBytes(20);
            if (!Arrays.equals(magic, JUNIT_MAGIC)
                    || schema != 1
                    || flags != 0
                    || tests <= 0
                    || failures < 0
                    || errors < 0
                    || skipped < 0
                    || reportLength <= 0
                    || reportLength != attachment.payloadLength() - 64
                    || !allZero(reserved)) {
                throw invalid("allocator JUnit attachment header/count is non-canonical");
            }
            byte[] xml = input.readNBytes(reportLength);
            if (xml.length != reportLength || input.read() != -1) {
                throw invalid("allocator JUnit attachment XML length differs");
            }
            AllocatorJUnitEvidenceV1.Counts recomputed =
                    AllocatorJUnitEvidenceV1.parse(new java.io.ByteArrayInputStream(xml));
            if (tests != recomputed.tests()
                    || failures != recomputed.failures()
                    || errors != recomputed.errors()
                    || skipped != recomputed.skipped()) {
                throw invalid("allocator JUnit attachment caller counts differ from exact XML recomputation");
            }
            return recomputed;
        } catch (IOException error) {
            throw invalid("allocator JUnit attachment could not be parsed", error);
        }
    }

    private static void readEvents(
            AllocatorEvidenceAttachmentV1 attachment,
            Map<Integer, IntervalAccumulator> intervals,
            FaultMatrix faults,
            LongOwnerTable allocatedIds) {
        try (DataInputStream input = new DataInputStream(attachment.openPayload())) {
            byte[] magic = input.readNBytes(4);
            int schema = input.readUnsignedShort();
            int kind = input.readUnsignedShort();
            long eventCount = input.readLong();
            byte[] reserved = input.readNBytes(16);
            if (!Arrays.equals(magic, EVENT_MAGIC)
                    || schema != 1
                    || kind != attachment.kind().code()
                    || eventCount <= 0
                    || attachment.payloadLength()
                            != EVENT_HEADER_BYTES + Math.multiplyExact(eventCount, AllocatorRawEvidenceEventV1.BYTES)
                    || !allZero(reserved)) {
                throw invalid("allocator raw event attachment header/count is non-canonical");
            }
            byte[] bytes = new byte[AllocatorRawEvidenceEventV1.BYTES];
            ByteBuffer eventBuffer = ByteBuffer.wrap(bytes);
            for (long index = 0; index < eventCount; index++) {
                input.readFully(bytes);
                eventBuffer.clear();
                AllocatorRawEvidenceEventV1 event = AllocatorRawEvidenceEventV1.decode(eventBuffer);
                requireAttachmentContainsEvent(attachment.kind(), event);
                if (attachment.kind() == AllocatorEvidenceAttachmentKindV1.FAULT) {
                    faults.add(event);
                } else {
                    int contextId = event.context().contextId();
                    IntervalAccumulator accumulator =
                            intervals.computeIfAbsent(contextId, ignored -> new IntervalAccumulator(event.context()));
                    accumulator.add(event);
                    if (event.kind() == EventKind.ALLOCATED_LEDGER_ID && event.allocatedLedgerId() > 0) {
                        int priorContext = allocatedIds.putIfAbsent(event.allocatedLedgerId(), contextId);
                        if (priorContext == contextId) {
                            accumulator.recordDuplicateLedgerId();
                        } else if (priorContext >= 0) {
                            accumulator.recordReusedLedgerId();
                            IntervalAccumulator prior = intervals.get(priorContext);
                            if (prior == null) {
                                throw invalid("allocator raw ledger ID owner context is absent");
                            }
                            prior.recordReusedLedgerId();
                        }
                    }
                }
            }
            if (input.read() != -1) {
                throw invalid("allocator raw event attachment has trailing bytes");
            }
        } catch (IOException | ArithmeticException error) {
            throw invalid("allocator raw event attachment could not be parsed", error);
        }
    }

    private static void requireAttachmentContainsEvent(
            AllocatorEvidenceAttachmentKindV1 kind, AllocatorRawEvidenceEventV1 event) {
        boolean exact =
                switch (kind) {
                    case TEST -> false;
                    case NATIVE -> event.context().nativePath();
                    case FAULT -> !event.context().nativePath();
                    case SCALE_10K ->
                        !event.context().nativePath() && event.context().activeManagedLedgers() == 10_000;
                    case SCALE_100K ->
                        !event.context().nativePath() && event.context().activeManagedLedgers() == 100_000;
                };
        if (!exact) {
            throw invalid("allocator raw event context appears in a different closed attachment path");
        }
    }

    private static final class IntervalAccumulator {
        private final AllocatorEvidenceContextV1 context;
        private final int warmupCount;
        private final int requestCount;
        private final long[] expectedLedgers;
        private final long[] offered;
        private final long[] enqueued;
        private final long[] dispatched;
        private final long[] admitted;
        private final long[] completed;
        private final long[] appendStart;
        private final long[] appendRelease;
        private final long[] allocated;
        private final long[] grantUse;
        private final long[] queueEnqueue;
        private final long[] queueDequeue;
        private final long[] queueEnqueueDepth;
        private final long[] queueDequeueDepth;
        private final byte[] terminal;
        private final int[] oxiaStartMask;
        private final int[] oxiaEndMask;
        private final long[] firstOxiaStart;
        private final long[] lastOxiaEnd;
        private final Map<Long, PendingOxiaOperation> pendingOxia = new HashMap<>();
        private final int[] staleBurnsByLedger;
        private final LongCollector oxiaDurations = new LongCollector();
        private long queueDepthMaximum;
        private long badEvents;
        private long failedAssertions;
        private long skippedAssertions;
        private long unexpectedErrors;
        private long grantWaste;
        private long grantUseOperations;
        private long staleCandidateBurns;
        private long staleCandidateBurnExcess;
        private long permanentOrphans;
        private long duplicateLedgerIds;
        private long reusedLedgerIds;
        private long metadataOperationCalls;
        private long metadataRequestBytes;
        private long metadataResponseBytes;

        private IntervalAccumulator(AllocatorEvidenceContextV1 context) {
            this.context = context;
            this.warmupCount = context.offeredRolloverRequestsPerSecond() * WARMUP_SECONDS;
            this.requestCount = context.offeredRolloverRequestsPerSecond() * (WARMUP_SECONDS + MEASURED_SECONDS);
            this.expectedLedgers = new long[requestCount];
            AllocatorEvidenceScheduleV1.Cursor cursor =
                    AllocatorEvidenceScheduleV1.ledgerCursor(context.activeManagedLedgers());
            for (int index = 0; index < requestCount; index++) {
                expectedLedgers[index] = cursor.nextLedgerIndex();
            }
            offered = emptyTimes();
            enqueued = emptyTimes();
            dispatched = emptyTimes();
            admitted = emptyTimes();
            completed = emptyTimes();
            appendStart = emptyTimes();
            appendRelease = emptyTimes();
            allocated = emptyTimes();
            grantUse = emptyTimes();
            queueEnqueue = emptyTimes();
            queueDequeue = emptyTimes();
            queueEnqueueDepth = emptyTimes();
            queueDequeueDepth = emptyTimes();
            terminal = new byte[requestCount];
            oxiaStartMask = context.nativePath() ? new int[0] : new int[requestCount];
            oxiaEndMask = context.nativePath() ? new int[0] : new int[requestCount];
            firstOxiaStart = context.nativePath() ? new long[0] : emptyTimes();
            lastOxiaEnd = context.nativePath() ? new long[0] : emptyTimes();
            staleBurnsByLedger = context.nativePath() ? new int[0] : new int[context.activeManagedLedgers()];
        }

        private long[] emptyTimes() {
            long[] values = new long[requestCount];
            Arrays.fill(values, -1);
            return values;
        }

        private void add(AllocatorRawEvidenceEventV1 event) {
            int ordinal = Math.toIntExact(event.requestOrdinal());
            if (ordinal >= requestCount
                    || event.actorId() != AllocatorEvidenceScheduleV1.actorId(ordinal)
                    || event.trigger() != AllocatorEvidenceScheduleV1.trigger(ordinal)
                    || event.managedLedgerIndex() != expectedLedgers[ordinal]
                    || ((event.flags() & AllocatorRawEvidenceEventV1.FLAG_WARMUP) != 0) != (ordinal < warmupCount)
                    || (event.flags() & AllocatorRawEvidenceEventV1.FLAG_FAULT_CUT_MASK) != 0) {
                throw invalid("allocator raw request actor/trigger/ledger/warmup schedule differs");
            }
            switch (event.kind()) {
                case OFFERED -> setOnce(offered, ordinal, event, true);
                case ENQUEUED -> setOnce(enqueued, ordinal, event, true);
                case DISPATCHED -> setOnce(dispatched, ordinal, event, true);
                case ADMITTED -> setOnce(admitted, ordinal, event, true);
                case COMPLETED -> setTerminal(ordinal, event, EventOutcome.SUCCESS);
                case FENCED -> setTerminal(ordinal, event, EventOutcome.FENCED);
                case FAILED -> setTerminal(ordinal, event, EventOutcome.FAILED);
                case TIMED_OUT -> setTerminal(ordinal, event, EventOutcome.TIMED_OUT);
                case APPEND_ADMISSION_START -> setOnce(appendStart, ordinal, event, true);
                case APPEND_ADMISSION_RELEASE -> setOnce(appendRelease, ordinal, event, true);
                case QUEUE_DEPTH -> queueDepth(ordinal, event);
                case OXIA_OPERATION_START -> oxiaStart(ordinal, event);
                case OXIA_OPERATION_END -> oxiaEnd(ordinal, event);
                case ALLOCATED_LEDGER_ID -> {
                    if (event.outcome() != EventOutcome.NONE
                            || event.value1() != 0
                            || event.value2() != 0
                            || event.allocatedLedgerId() <= 0
                            || (context.nativePath() ? event.ownerEpoch() != 0 : event.ownerEpoch() <= 0)
                            || hasOxiaFlags(event)) {
                        throw invalid("allocator allocated-ledger-ID event shape is invalid");
                    }
                    if (allocated[ordinal] >= 0) {
                        throw invalid("allocator raw request repeats allocated ledger ID");
                    }
                    allocated[ordinal] = event.allocatedLedgerId();
                }
                case GRANT_USE -> grantUse(ordinal, event);
                case GRANT_WASTE -> grantWaste(ordinal, event);
                case STALE_CANDIDATE_BURN -> staleCandidateBurn(ordinal, event);
                case PERMANENT_ORPHAN -> permanentOrphan(ordinal, event);
                case ASSERTION_FAILED -> {
                    failedAssertions++;
                    badEvents++;
                }
                case TEST_SKIPPED -> {
                    skippedAssertions++;
                    badEvents++;
                }
                case UNEXPECTED_ERROR -> {
                    unexpectedErrors++;
                    badEvents++;
                }
                default -> throw invalid("allocator measurement attachment contains a fault-only event kind");
            }
        }

        private void queueDepth(int ordinal, AllocatorRawEvidenceEventV1 event) {
            if (event.outcome() != EventOutcome.NONE
                    || event.value1() < 0
                    || (event.value2() != AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE
                            && event.value2() != AllocatorRawEvidenceEventV1.QUEUE_DEQUEUE)
                    || event.allocatedLedgerId() != 0
                    || event.ownerEpoch() != 0
                    || hasOxiaFlags(event)) {
                throw invalid("allocator queue-depth event shape is invalid");
            }
            long[] target = event.value2() == AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE ? queueEnqueue : queueDequeue;
            long[] depthTarget =
                    event.value2() == AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE ? queueEnqueueDepth : queueDequeueDepth;
            if (target[ordinal] >= 0) {
                throw invalid("allocator raw request repeats an enqueue/dequeue queue-depth sample");
            }
            target[ordinal] = event.monotonicTimestampMicros();
            depthTarget[ordinal] = event.value1();
            if (ordinal >= warmupCount) {
                queueDepthMaximum = Math.max(queueDepthMaximum, event.value1());
            }
        }

        private void grantUse(int ordinal, AllocatorRawEvidenceEventV1 event) {
            requireCandidateEvent(event);
            if (event.outcome() != EventOutcome.NONE
                    || event.value1() <= 0
                    || event.value2() != 0
                    || event.allocatedLedgerId() <= 0
                    || event.ownerEpoch() <= 0
                    || grantUse[ordinal] >= 0) {
                throw invalid("allocator grant-use event shape is invalid or duplicated");
            }
            grantUse[ordinal] = event.allocatedLedgerId();
            if (ordinal >= warmupCount) {
                grantUseOperations++;
            }
        }

        private void grantWaste(int ordinal, AllocatorRawEvidenceEventV1 event) {
            requireCandidateEvent(event);
            if (event.outcome() != EventOutcome.NONE
                    || event.value1() <= 0
                    || event.value2() <= 0
                    || event.allocatedLedgerId() != 0
                    || event.ownerEpoch() <= 0) {
                throw invalid("allocator grant-waste event shape is invalid");
            }
            if (ordinal >= warmupCount) {
                grantWaste = Math.addExact(grantWaste, event.value2());
            }
        }

        private void staleCandidateBurn(int ordinal, AllocatorRawEvidenceEventV1 event) {
            requireCandidateEvent(event);
            if (event.outcome() != EventOutcome.NONE
                    || event.value1() <= 0
                    || event.value2() != 0
                    || event.allocatedLedgerId() <= 0
                    || event.ownerEpoch() <= 0) {
                throw invalid("allocator stale-candidate-burn event shape is invalid");
            }
            int ledger = Math.toIntExact(event.managedLedgerIndex());
            staleBurnsByLedger[ledger]++;
            if (ordinal >= warmupCount) {
                staleCandidateBurns++;
                if (staleBurnsByLedger[ledger] > 1) {
                    staleCandidateBurnExcess++;
                }
            }
        }

        private void permanentOrphan(int ordinal, AllocatorRawEvidenceEventV1 event) {
            requireCandidateEvent(event);
            if (event.outcome() != EventOutcome.NONE
                    || event.value1() != 0
                    || event.value2() != 0
                    || event.allocatedLedgerId() <= 0
                    || event.ownerEpoch() != 0) {
                throw invalid("allocator permanent-orphan event shape is invalid");
            }
            if (ordinal >= warmupCount) {
                permanentOrphans++;
            }
        }

        private void requireCandidateEvent(AllocatorRawEvidenceEventV1 event) {
            if (context.nativePath()
                    || event.oxiaOperationKind() != OxiaOperationKind.NONE
                    || event.operationSequence() != 0) {
                throw invalid("allocator candidate-only event appears in native or aliases Oxia flags");
            }
        }

        private void oxiaStart(int ordinal, AllocatorRawEvidenceEventV1 event) {
            if (context.nativePath()
                    || event.oxiaOperationKind() == OxiaOperationKind.NONE
                    || event.outcome() != EventOutcome.NONE
                    || event.value2() <= 0
                    || event.allocatedLedgerId() != 0
                    || event.ownerEpoch() != 0) {
                throw invalid("allocator Oxia start event shape is invalid");
            }
            int bit = 1 << event.operationSequence();
            if ((oxiaStartMask[ordinal] & bit) != 0) {
                throw invalid("allocator raw request repeats an Oxia operation start");
            }
            oxiaStartMask[ordinal] |= bit;
            long operationKey = operationKey(ordinal, event.operationSequence());
            if (pendingOxia.putIfAbsent(
                            operationKey,
                            new PendingOxiaOperation(
                                    event.monotonicTimestampMicros(), event.value2(), event.oxiaOperationKind()))
                    != null) {
                throw invalid("allocator raw request repeats one pending Oxia operation");
            }
            firstOxiaStart[ordinal] = firstOxiaStart[ordinal] < 0
                    ? event.monotonicTimestampMicros()
                    : Math.min(firstOxiaStart[ordinal], event.monotonicTimestampMicros());
            if (ordinal >= warmupCount) {
                metadataRequestBytes = Math.addExact(metadataRequestBytes, event.value1());
            }
        }

        private void oxiaEnd(int ordinal, AllocatorRawEvidenceEventV1 event) {
            if (context.nativePath()
                    || event.oxiaOperationKind() == OxiaOperationKind.NONE
                    || event.outcome() == EventOutcome.NONE
                    || event.value2() <= 0
                    || event.allocatedLedgerId() != 0
                    || event.ownerEpoch() != 0) {
                throw invalid("allocator Oxia end event shape is invalid");
            }
            int bit = 1 << event.operationSequence();
            if ((oxiaEndMask[ordinal] & bit) != 0) {
                throw invalid("allocator raw request repeats an Oxia operation end");
            }
            oxiaEndMask[ordinal] |= bit;
            PendingOxiaOperation start = pendingOxia.remove(operationKey(ordinal, event.operationSequence()));
            if (start == null
                    || start.token() != event.value2()
                    || start.kind() != event.oxiaOperationKind()
                    || event.monotonicTimestampMicros() < start.timestampMicros()) {
                throw invalid("allocator raw Oxia operation end differs from its exact preceding start");
            }
            lastOxiaEnd[ordinal] = Math.max(lastOxiaEnd[ordinal], event.monotonicTimestampMicros());
            if (ordinal >= warmupCount) {
                metadataResponseBytes = Math.addExact(metadataResponseBytes, event.value1());
                oxiaDurations.add(event.monotonicTimestampMicros() - start.timestampMicros());
                metadataOperationCalls++;
            }
        }

        private void setTerminal(int ordinal, AllocatorRawEvidenceEventV1 event, EventOutcome expected) {
            requireEventValueShape(event, event.outcome() == expected);
            if (terminal[ordinal] != 0) {
                throw invalid("allocator raw request has more than one terminal result");
            }
            terminal[ordinal] = (byte) expected.ordinal();
            completed[ordinal] = event.monotonicTimestampMicros();
        }

        private static void setOnce(
                long[] target, int ordinal, AllocatorRawEvidenceEventV1 event, boolean requireEmptyValues) {
            if (target[ordinal] >= 0) {
                throw invalid("allocator raw request repeats one lifecycle endpoint");
            }
            if (requireEmptyValues) {
                requireEventValueShape(event, event.outcome() == EventOutcome.NONE);
            }
            target[ordinal] = event.monotonicTimestampMicros();
        }

        private IntervalSummary finish() {
            validateQueueDepthReplay();
            LongCollector endToEnd = new LongCollector();
            LongCollector queueAge = new LongCollector();
            LongCollector appendStall = new LongCollector();
            long starvation = 0;
            long successful = 0;
            long fenced = 0;
            long errors = 0;
            long timedOut = 0;
            boolean drainedInsideMeasuredInterval = true;
            long baseOffered = offered[0];
            AllocatorEvidenceScheduleV1.ArrivalCursor arrival =
                    AllocatorEvidenceScheduleV1.arrivalCursor(context.offeredRolloverRequestsPerSecond());
            for (int ordinal = 0; ordinal < requestCount; ordinal++) {
                if (offered[ordinal] < 0
                        || enqueued[ordinal] < offered[ordinal]
                        || dispatched[ordinal] < enqueued[ordinal]
                        || admitted[ordinal] < dispatched[ordinal]
                        || appendStart[ordinal] < admitted[ordinal]
                        || appendRelease[ordinal] < appendStart[ordinal]
                        || completed[ordinal] < appendRelease[ordinal]
                        || terminal[ordinal] == 0
                        || queueEnqueue[ordinal] != enqueued[ordinal]
                        || queueDequeue[ordinal] != dispatched[ordinal]) {
                    throw invalid("allocator raw request lacks ordered independent lifecycle endpoints");
                }
                EventOutcome outcome = EventOutcome.values()[terminal[ordinal]];
                if ((outcome == EventOutcome.SUCCESS && allocated[ordinal] <= 0)
                        || (!context.nativePath() && allocated[ordinal] > 0 && grantUse[ordinal] != allocated[ordinal])
                        || (grantUse[ordinal] > 0 && allocated[ordinal] != grantUse[ordinal])) {
                    throw invalid("allocator raw terminal/allocation/grant-use association differs");
                }
                long expectedOffered = arrival.nextOfferedTimestampMicros();
                if (offered[ordinal] - baseOffered != expectedOffered) {
                    throw invalid("allocator raw offered timestamps differ from deterministic warmup/storm schedule");
                }
                if (ordinal >= warmupCount) {
                    if (outcome == EventOutcome.SUCCESS) {
                        successful++;
                    } else if (outcome == EventOutcome.FENCED) {
                        fenced++;
                    } else if (outcome == EventOutcome.TIMED_OUT) {
                        timedOut++;
                    } else {
                        errors++;
                    }
                    endToEnd.add(completed[ordinal] - offered[ordinal]);
                    queueAge.add(dispatched[ordinal] - offered[ordinal]);
                    appendStall.add(appendRelease[ordinal] - appendStart[ordinal]);
                    starvation = Math.max(starvation, completed[ordinal] - offered[ordinal]);
                    if (completed[ordinal]
                            > Math.addExact(baseOffered, (WARMUP_SECONDS + MEASURED_SECONDS) * 1_000_000L)) {
                        drainedInsideMeasuredInterval = false;
                    }
                }
            }
            if (!context.nativePath()) {
                if (!pendingOxia.isEmpty()) {
                    throw invalid("allocator raw Oxia operation stream has an unmatched start");
                }
                for (int ordinal = 0; ordinal < requestCount; ordinal++) {
                    if (oxiaStartMask[ordinal] != oxiaEndMask[ordinal]
                            || (oxiaStartMask[ordinal] != 0
                                    && (firstOxiaStart[ordinal] < admitted[ordinal]
                                            || lastOxiaEnd[ordinal] > completed[ordinal]))) {
                        throw invalid("allocator raw Oxia operations have unpaired/reversed endpoints");
                    }
                }
                if (oxiaDurations.size == 0) {
                    throw invalid("allocator raw Oxia operations are absent");
                }
            }
            long measured = (long) context.offeredRolloverRequestsPerSecond() * MEASURED_SECONDS;
            boolean completeZero = successful == measured
                    && fenced == 0
                    && errors == 0
                    && timedOut == 0
                    && badEvents == 0
                    && duplicateLedgerIds == 0
                    && reusedLedgerIds == 0
                    && staleCandidateBurnExcess == 0
                    && drainedInsideMeasuredInterval;
            return new IntervalSummary(
                    context,
                    completeZero,
                    endToEnd.p99(),
                    context.nativePath() ? 0 : oxiaDurations.p99(),
                    queueDepthMaximum,
                    queueAge.p99(),
                    starvation,
                    appendStall.p99(),
                    successful,
                    fenced,
                    errors,
                    timedOut,
                    permanentOrphans,
                    duplicateLedgerIds,
                    reusedLedgerIds,
                    staleCandidateBurns,
                    staleCandidateBurnExcess,
                    metadataOperationCalls,
                    metadataRequestBytes,
                    metadataResponseBytes,
                    grantUseOperations,
                    grantWaste,
                    failedAssertions,
                    skippedAssertions,
                    unexpectedErrors);
        }

        /**
         * Replays the caller-reported queue-depth transitions. Events at one microsecond may be physically reordered
         * by the concurrent segmented writer, so a same-timestamp group is admitted only when its exact directed
         * transitions have an Euler trail from the preceding depth. This rejects forged low depths without inventing
         * an ordering that the raw event fields cannot prove.
         */
        private void validateQueueDepthReplay() {
            long[] timestamps = new long[Math.multiplyExact(requestCount, 2)];
            long[] actions = new long[timestamps.length];
            long[] depths = new long[timestamps.length];
            for (int ordinal = 0; ordinal < requestCount; ordinal++) {
                if (queueEnqueue[ordinal] < 0
                        || queueDequeue[ordinal] < 0
                        || queueEnqueueDepth[ordinal] < 0
                        || queueDequeueDepth[ordinal] < 0
                        || queueEnqueueDepth[ordinal] > requestCount
                        || queueDequeueDepth[ordinal] > requestCount) {
                    throw invalid("allocator raw queue-depth transition inventory is incomplete or out of bounds");
                }
                timestamps[ordinal * 2] = queueEnqueue[ordinal];
                actions[ordinal * 2] = AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE;
                depths[ordinal * 2] = queueEnqueueDepth[ordinal];
                timestamps[ordinal * 2 + 1] = queueDequeue[ordinal];
                actions[ordinal * 2 + 1] = AllocatorRawEvidenceEventV1.QUEUE_DEQUEUE;
                depths[ordinal * 2 + 1] = queueDequeueDepth[ordinal];
            }
            validateQueueDepthTransitions(timestamps, actions, depths);
        }

        private void recordDuplicateLedgerId() {
            duplicateLedgerIds++;
        }

        private void recordReusedLedgerId() {
            reusedLedgerIds++;
        }

        private static long operationKey(int requestOrdinal, int operationSequence) {
            return ((long) requestOrdinal << OPERATION_SEQUENCE_BITS) | operationSequence;
        }

        private record PendingOxiaOperation(long timestampMicros, long token, OxiaOperationKind kind) {}
    }

    private static final class FaultMatrix {
        private final Map<FaultKey, FaultAccumulator> entries = new HashMap<>();

        private void add(AllocatorRawEvidenceEventV1 event) {
            if (event.context().nativePath()
                    || event.context().offeredRolloverRequestsPerSecond() != 200
                    || (event.flags() & AllocatorRawEvidenceEventV1.FLAG_WARMUP) != 0) {
                throw invalid("allocator fault event has a native/non-200/warmup context");
            }
            AllocatorFaultCutV1 cut = AllocatorFaultCutV1.fromFlags(event.flags());
            FaultKey key = new FaultKey(
                    event.context().candidate(),
                    event.context().activeManagedLedgers(),
                    event.context().metadataLatencyP99Millis(),
                    cut);
            entries.computeIfAbsent(key, ignored -> new FaultAccumulator(key)).add(event);
        }

        private Map<RowKey, FaultRowSummary> finish() {
            if (entries.size() != 5 * 2 * 4 * 9) {
                throw invalid("allocator fault attachment does not contain every 5x8x9 cut tuple");
            }
            Map<RowKey, FaultRowSummary> rows = new HashMap<>();
            for (FaultAccumulator entry : entries.values()) {
                entry.finish();
                RowKey rowKey = RowKey.candidateRow(entry.key.candidate, entry.key.population, entry.key.latency);
                FaultRowSummary row = rows.computeIfAbsent(rowKey, ignored -> new FaultRowSummary());
                row.cuts++;
                row.failedAssertions += entry.failedAssertions;
                row.skippedAssertions += entry.skippedAssertions;
                row.unexpectedErrors += entry.unexpectedErrors;
                row.failedOperations += entry.failedOperations;
                row.timedOutOperations += entry.timedOutOperations;
                row.staleBurnExcess += entry.staleBurnExcess;
                row.staleCandidateBurns += entry.burnedLedgerIds.size();
                if (entry.key.cut == AllocatorFaultCutV1.BROKER_SESSION_CRASH_MASS_TAKEOVER) {
                    row.recoveryMicros = entry.recoveryMicros();
                }
            }
            if (rows.size() != 5 * 8) {
                throw invalid("allocator fault attachment row inventory differs from five candidates x eight rows");
            }
            return rows;
        }
    }

    private static final class FaultAccumulator {
        private final FaultKey key;
        private final Map<Long, WriteProof> dispatched = new HashMap<>();
        private final Map<Long, WriteProof> reread = new HashMap<>();
        private final Map<Long, WriteProof> terminal = new HashMap<>();
        private final Map<Long, EventOutcome> terminalOutcomes = new HashMap<>();
        private final Set<Long> burnedLedgerIds = new HashSet<>();
        private final BitSet freshOwnerLedgers;
        private final byte[] freshOwnerActors;
        private final int[] staleBurnsByLedger;
        private long begin = -1;
        private long end = -1;
        private long ownerLoss = -1;
        private long lastFreshAppend = -1;
        private int failedActor = -1;
        private long failedAssertions;
        private long skippedAssertions;
        private long unexpectedErrors;
        private long failedOperations;
        private long timedOutOperations;
        private long staleBurnExcess;

        private FaultAccumulator(FaultKey key) {
            this.key = key;
            this.freshOwnerLedgers = new BitSet(key.population);
            this.freshOwnerActors = new byte[key.population];
            Arrays.fill(freshOwnerActors, (byte) -1);
            this.staleBurnsByLedger = new int[key.population];
        }

        private void add(AllocatorRawEvidenceEventV1 event) {
            switch (event.kind()) {
                case CUT_BEGIN -> begin = cutBoundary(begin, event, "cut begin");
                case CUT_END -> end = cutBoundary(end, event, "cut end");
                case METADATA_WRITE_DISPATCH -> addWriteProof(dispatched, event, EventOutcome.NONE);
                case SAME_KEY_REREAD -> addWriteProof(reread, event, EventOutcome.NONE);
                case TYPED_TERMINAL_DISPOSITION -> {
                    if (event.outcome() != EventOutcome.APPLIED_EXACT
                            && event.outcome() != EventOutcome.PREDECESSOR_UNCHANGED
                            && event.outcome() != EventOutcome.CONFLICT) {
                        throw invalid("allocator fault terminal disposition is untyped");
                    }
                    addWriteProof(terminal, event, event.outcome());
                    terminalOutcomes.put(event.value1(), event.outcome());
                }
                case OWNER_LOSS_DETECTED -> ownerLoss(event);
                case FRESH_OWNER_APPEND_COMPLETE -> freshOwnerAppend(event);
                case STALE_CANDIDATE_BURN -> {
                    if (event.outcome() != EventOutcome.NONE
                            || event.value1() <= 0
                            || event.value2() != 0
                            || event.allocatedLedgerId() <= 0
                            || event.ownerEpoch() <= 0
                            || hasOxiaFlags(event)) {
                        throw invalid("allocator fault stale-candidate burn has no ledger ID");
                    }
                    int ledger = Math.toIntExact(event.managedLedgerIndex());
                    staleBurnsByLedger[ledger]++;
                    if (staleBurnsByLedger[ledger] > 1) {
                        staleBurnExcess++;
                    }
                    if (!burnedLedgerIds.add(event.allocatedLedgerId())) {
                        staleBurnExcess++;
                    }
                }
                case ASSERTION_FAILED -> failedAssertions++;
                case TEST_SKIPPED -> skippedAssertions++;
                case UNEXPECTED_ERROR -> unexpectedErrors++;
                case FAILED -> failedOperations++;
                case TIMED_OUT -> timedOutOperations++;
                default -> {
                    // Lifecycle/Oxia/grant events are retained raw but do not replace write-reread-terminal proof.
                }
            }
        }

        private void finish() {
            if (begin < 0
                    || end < begin
                    || dispatched.isEmpty()
                    || !reread.equals(dispatched)
                    || !terminal.equals(dispatched)) {
                throw invalid("allocator fault cut lacks exact write/reread/typed-terminal proof");
            }
            requireCutSpecificOperations();
            if (key.cut == AllocatorFaultCutV1.BROKER_SESSION_CRASH_MASS_TAKEOVER
                    && (failedActor < 0
                            || ownerLoss < begin
                            || lastFreshAppend < ownerLoss
                            || lastFreshAppend > end
                            || freshOwnerLedgers.cardinality() != key.population / 4)) {
                throw invalid("allocator mass-takeover cut lacks exact affected-ledger recovery inventory");
            }
            if (key.cut == AllocatorFaultCutV1.BROKER_SESSION_CRASH_MASS_TAKEOVER) {
                for (int ledger = failedActor; ledger < key.population; ledger += 4) {
                    if (!freshOwnerLedgers.get(ledger) || freshOwnerActors[ledger] == failedActor) {
                        throw invalid("allocator mass-takeover cut omits one failed-actor ledger");
                    }
                }
                for (int ledger = freshOwnerLedgers.nextSetBit(0);
                        ledger >= 0;
                        ledger = freshOwnerLedgers.nextSetBit(ledger + 1)) {
                    if (ledger % 4 != failedActor) {
                        throw invalid("allocator mass-takeover includes a ledger not owned by the failed actor");
                    }
                }
            }
            if (key.cut != AllocatorFaultCutV1.BROKER_SESSION_CRASH_MASS_TAKEOVER
                    && (ownerLoss >= 0 || !freshOwnerLedgers.isEmpty())) {
                throw invalid("allocator non-mass fault cut contains mass-takeover endpoints");
            }
        }

        private long recoveryMicros() {
            return lastFreshAppend - ownerLoss;
        }

        private void ownerLoss(AllocatorRawEvidenceEventV1 event) {
            requireEventValueShape(event, event.outcome() == EventOutcome.NONE);
            ownerLoss = unique(ownerLoss, event.monotonicTimestampMicros(), "owner loss detection");
            failedActor = event.actorId();
        }

        private void freshOwnerAppend(AllocatorRawEvidenceEventV1 event) {
            if (event.outcome() != EventOutcome.SUCCESS
                    || event.value1() != 0
                    || event.value2() != 0
                    || event.allocatedLedgerId() != 0
                    || event.ownerEpoch() <= 0
                    || hasOxiaFlags(event)) {
                throw invalid("allocator fresh-owner append event shape is invalid");
            }
            int ledger = Math.toIntExact(event.managedLedgerIndex());
            if (freshOwnerLedgers.get(ledger)) {
                throw invalid("allocator mass-takeover repeats one affected-ledger completion");
            }
            freshOwnerLedgers.set(ledger);
            freshOwnerActors[ledger] = (byte) event.actorId();
            lastFreshAppend = Math.max(lastFreshAppend, event.monotonicTimestampMicros());
        }

        private static void addWriteProof(
                Map<Long, WriteProof> target, AllocatorRawEvidenceEventV1 event, EventOutcome requiredOutcome) {
            if (event.value1() <= 0
                    || event.value2() <= 0
                    || event.allocatedLedgerId() != 0
                    || event.ownerEpoch() <= 0
                    || event.outcome() != requiredOutcome
                    || event.oxiaOperationKind() == OxiaOperationKind.NONE
                    || target.putIfAbsent(
                                    event.value1(),
                                    new WriteProof(
                                            event.oxiaOperationKind(),
                                            event.operationSequence(),
                                            event.value2(),
                                            event.ownerEpoch()))
                            != null) {
                throw invalid("allocator fault write proof token is zero or duplicated");
            }
        }

        private void requireCutSpecificOperations() {
            Set<OxiaOperationKind> kinds = new HashSet<>();
            dispatched.values().forEach(proof -> kinds.add(proof.kind));
            OxiaOperationKind required =
                    switch (key.cut) {
                        case RESERVE_RESPONSE_LOSS -> OxiaOperationKind.CELL_RESERVE_CAS;
                        case MODE_GRANT_READY_RESPONSE_LOSS_OR_STRICT_NO_INSTALL ->
                            key.candidate.mode() == AllocatorModeV1.RANGE_LEASED
                                    ? OxiaOperationKind.RANGE_GRANT_INSTALL_CAS
                                    : null;
                        case NODE_CREATE_RESPONSE_LOSS -> OxiaOperationKind.NODE_CREATE;
                        case HEAD_PUBLISH_RESPONSE_LOSS, LATE_OLD_OWNER_WRITE, SYNCHRONIZED_STORM ->
                            OxiaOperationKind.HEAD_PUBLISH_CAS;
                        case CELL_CLEAR_RESPONSE_LOSS -> OxiaOperationKind.CELL_CLEAR_CAS;
                        case SINGLE_OWNER_TAKEOVER, BROKER_SESSION_CRASH_MASS_TAKEOVER ->
                            OxiaOperationKind.HEAD_TAKEOVER_CAS;
                    };
            if (required != null && !kinds.contains(required)) {
                throw invalid("allocator fault cut omits its mode/cut-specific production mutation");
            }
            boolean exactLateOwnerConflict = false;
            for (Map.Entry<Long, WriteProof> entry : terminal.entrySet()) {
                EventOutcome outcome = terminalOutcomes.get(entry.getKey());
                boolean lateOwnerPublish = key.cut == AllocatorFaultCutV1.LATE_OLD_OWNER_WRITE
                        && entry.getValue().kind == OxiaOperationKind.HEAD_PUBLISH_CAS;
                if (lateOwnerPublish && outcome == EventOutcome.CONFLICT) {
                    exactLateOwnerConflict = true;
                } else if (outcome == EventOutcome.CONFLICT) {
                    throw invalid("allocator non-late-owner fault mutation ended in an unexplained conflict");
                }
            }
            if (key.cut == AllocatorFaultCutV1.LATE_OLD_OWNER_WRITE && !exactLateOwnerConflict) {
                throw invalid("allocator late old-owner write lacks a typed definitive conflict");
            }
            if (key.cut == AllocatorFaultCutV1.MODE_GRANT_READY_RESPONSE_LOSS_OR_STRICT_NO_INSTALL
                    && key.candidate.mode() == AllocatorModeV1.STRICT_SERIALIZED) {
                if (kinds.contains(OxiaOperationKind.RANGE_GRANT_INSTALL_CAS)
                        || !kinds.containsAll(Set.of(
                                OxiaOperationKind.CELL_RESERVE_CAS,
                                OxiaOperationKind.NODE_CREATE,
                                OxiaOperationKind.HEAD_PUBLISH_CAS,
                                OxiaOperationKind.CELL_CLEAR_CAS))) {
                    throw invalid("STRICT no-install cut does not prove the inseparable four-write path");
                }
            }
        }

        private static long unique(long existing, long candidate, String field) {
            if (existing >= 0) {
                throw invalid("allocator fault repeats " + field);
            }
            return candidate;
        }

        private static long cutBoundary(long existing, AllocatorRawEvidenceEventV1 event, String field) {
            requireEventValueShape(event, event.outcome() == EventOutcome.NONE);
            return unique(existing, event.monotonicTimestampMicros(), field);
        }

        private record WriteProof(OxiaOperationKind kind, int sequence, long canonicalBytes, long ownerEpoch) {}
    }

    private record IntervalSummary(
            AllocatorEvidenceContextV1 context,
            boolean completeZeroFailure,
            long rolloverP99,
            long oxiaP99,
            long queueDepthMaximum,
            long queueAgeP99,
            long starvationMaximum,
            long appendP99,
            long successful,
            long fenced,
            long errors,
            long timedOut,
            long permanentOrphans,
            long duplicateLedgerIds,
            long reusedLedgerIds,
            long staleCandidateBurns,
            long staleCandidateBurnExcess,
            long metadataOperationCalls,
            long metadataRequestBytes,
            long metadataResponseBytes,
            long grantUseOperations,
            long grantWaste,
            long failedAssertions,
            long skippedAssertions,
            long unexpectedErrors) {
        private boolean absoluteBoundsPass() {
            int offered = context.offeredRolloverRequestsPerSecond();
            return completeZeroFailure
                    && offered >= 200
                    && rolloverP99 <= 250 * MICROS_PER_MILLI
                    && oxiaP99 <= 250 * MICROS_PER_MILLI
                    && queueAgeP99 <= 1_000 * MICROS_PER_MILLI
                    && queueDepthMaximum <= 2L * offered
                    && starvationMaximum <= 2_000 * MICROS_PER_MILLI
                    && appendP99 <= 2_000 * MICROS_PER_MILLI;
        }

        private AllocatorNativeRelativeMetricsV1 toMetrics(NativeRow nativeRow, FaultRowSummary fault) {
            return new AllocatorNativeRelativeMetricsV1(
                    new AllocatorEvidenceWorkloadV1(
                            context.candidate().mode(),
                            context.candidate().rangeSize(),
                            context.activeManagedLedgers(),
                            4,
                            context.metadataLatencyP99Millis()),
                    context.offeredRolloverRequestsPerSecond(),
                    nativeRow.sustainableRate,
                    rolloverP99,
                    oxiaP99,
                    queueDepthMaximum,
                    queueAgeP99,
                    starvationMaximum,
                    appendP99,
                    nativeRow.appendP99,
                    fault.recoveryMicros,
                    successful,
                    fenced,
                    errors,
                    timedOut,
                    permanentOrphans,
                    duplicateLedgerIds,
                    reusedLedgerIds,
                    failedAssertions + fault.failedAssertions,
                    skippedAssertions + fault.skippedAssertions,
                    unexpectedErrors + fault.unexpectedErrors,
                    metadataOperationCalls,
                    metadataRequestBytes,
                    metadataResponseBytes,
                    grantUseOperations,
                    grantWaste,
                    staleCandidateBurns + fault.staleCandidateBurns);
        }
    }

    /** Exact queue replay shared by the production validator and focused caller-forgery tests. */
    static void validateQueueDepthTransitions(long[] timestamps, long[] actions, long[] reportedDepths) {
        if (timestamps == null
                || actions == null
                || reportedDepths == null
                || timestamps.length == 0
                || timestamps.length != actions.length
                || timestamps.length != reportedDepths.length) {
            throw invalid("allocator raw queue-depth arrays differ or are empty");
        }
        QueuePoint[] points = new QueuePoint[timestamps.length];
        for (int index = 0; index < points.length; index++) {
            if (timestamps[index] < 0
                    || reportedDepths[index] < 0
                    || reportedDepths[index] > points.length
                    || (actions[index] != AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE
                            && actions[index] != AllocatorRawEvidenceEventV1.QUEUE_DEQUEUE)) {
                throw invalid("allocator raw queue-depth transition is outside its closed shape");
            }
            points[index] = new QueuePoint(timestamps[index], actions[index], reportedDepths[index]);
        }
        Arrays.sort(points, Comparator.comparingLong(QueuePoint::timestampMicros));
        long currentDepth = 0;
        for (int start = 0; start < points.length; ) {
            int end = start + 1;
            while (end < points.length && points[end].timestampMicros == points[start].timestampMicros) {
                end++;
            }
            currentDepth = replayQueueTimestampGroup(points, start, end, currentDepth);
            start = end;
        }
        if (currentDepth != 0) {
            throw invalid("allocator raw queue-depth replay does not drain to zero");
        }
    }

    private static long replayQueueTimestampGroup(QueuePoint[] points, int start, int end, long initialDepth) {
        Map<Long, int[]> outgoing = new HashMap<>();
        Map<Long, int[]> originalOutgoing = new HashMap<>();
        long enqueues = 0;
        long dequeues = 0;
        for (int index = start; index < end; index++) {
            QueuePoint point = points[index];
            long edgeStart;
            int edge;
            if (point.action == AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE) {
                if (point.reportedDepth <= 0) {
                    throw invalid("allocator queue enqueue reports a non-positive resulting depth");
                }
                edgeStart = point.reportedDepth - 1;
                edge = 0;
                enqueues++;
            } else {
                edgeStart = Math.addExact(point.reportedDepth, 1);
                edge = 1;
                dequeues++;
            }
            outgoing.computeIfAbsent(edgeStart, ignored -> new int[2])[edge]++;
            originalOutgoing.computeIfAbsent(edgeStart, ignored -> new int[2])[edge]++;
        }
        long finalDepth = initialDepth + enqueues - dequeues;
        if (finalDepth < 0) {
            throw invalid("allocator raw queue-depth replay underflows");
        }
        long[] stack = new long[end - start + 1];
        long[] reverseTrail = new long[end - start + 1];
        int stackSize = 1;
        int trailSize = 0;
        stack[0] = initialDepth;
        int used = 0;
        while (stackSize > 0) {
            long vertex = stack[stackSize - 1];
            int[] edges = outgoing.get(vertex);
            if (edges != null && edges[0] > 0) {
                edges[0]--;
                stack[stackSize++] = vertex + 1;
                used++;
            } else if (edges != null && edges[1] > 0) {
                edges[1]--;
                stack[stackSize++] = vertex - 1;
                used++;
            } else {
                reverseTrail[trailSize++] = vertex;
                stackSize--;
            }
        }
        if (used != end - start || trailSize != end - start + 1) {
            throw invalid("allocator raw same-timestamp queue-depth transitions have no exact replay");
        }
        long vertex = reverseTrail[trailSize - 1];
        if (vertex != initialDepth || reverseTrail[0] != finalDepth) {
            throw invalid("allocator raw same-timestamp queue-depth replay endpoints differ");
        }
        for (int index = trailSize - 2; index >= 0; index--) {
            long next = reverseTrail[index];
            int edge = next == vertex + 1 ? 0 : next == vertex - 1 ? 1 : -1;
            int[] edges = originalOutgoing.get(vertex);
            if (edge < 0 || edges == null || edges[edge] == 0) {
                throw invalid("allocator raw same-timestamp queue-depth replay invents a transition");
            }
            edges[edge]--;
            vertex = next;
        }
        return finalDepth;
    }

    private record QueuePoint(long timestampMicros, long action, long reportedDepth) {}

    private static final class LongCollector {
        private long[] values = new long[1024];
        private int size;

        private void add(long value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        private long p99() {
            if (size == 0) {
                return 0;
            }
            long[] exact = Arrays.copyOf(values, size);
            Arrays.sort(exact);
            int index = Math.max(0, (int) Math.ceil(size * 0.99) - 1);
            return exact[index];
        }
    }

    enum SelectionStatus {
        STRICT_SELECTED,
        RANGE_SELECTED,
        NONE_QUALIFIED,
        BOTH_QUALIFIED
    }

    record SelectionComputation(
            SelectionStatus status,
            Optional<AllocatorEvidenceCandidateV1> selectedCandidate,
            List<AllocatorNativeRelativeMetricsV1> selectedRows,
            List<AllocatorEvidenceCandidateV1> qualifiedCandidates,
            AllocatorJUnitEvidenceV1.Counts junit) {}

    private record CandidateQualification(
            AllocatorEvidenceCandidateV1 candidate, List<AllocatorNativeRelativeMetricsV1> rows) {}

    private record NativeRow(long sustainableRate, long appendP99) {}

    private record FaultKey(
            AllocatorEvidenceCandidateV1 candidate, int population, int latency, AllocatorFaultCutV1 cut) {}

    private record RowKey(AllocatorModeV1 mode, long range, int population, int latency) {
        private static RowKey nativeRow(int population, int latency) {
            return new RowKey(null, 0, population, latency);
        }

        private static RowKey candidateRow(AllocatorEvidenceCandidateV1 candidate, int population, int latency) {
            return new RowKey(candidate.mode(), candidate.rangeSize(), population, latency);
        }
    }

    private static final class FaultRowSummary {
        private int cuts;
        private long recoveryMicros = -1;
        private long failedAssertions;
        private long skippedAssertions;
        private long unexpectedErrors;
        private long failedOperations;
        private long timedOutOperations;
        private long staleBurnExcess;
        private long staleCandidateBurns;

        private boolean qualifies(int population) {
            return cuts == AllocatorSelectionReceiptV1.REQUIRED_FAULT_CUTS
                    && recoveryMicros >= 0
                    && recoveryMicros <= AllocatorEvidenceContextV1.massTakeoverRecoveryBoundMicros(population)
                    && failedAssertions == 0
                    && skippedAssertions == 0
                    && unexpectedErrors == 0
                    && failedOperations == 0
                    && timedOutOperations == 0
                    && staleBurnExcess == 0;
        }
    }

    /**
     * Primitive positive-ledger-ID to first-context table; avoids millions of boxed Long entries under the 6-GiB
     * cap.
     */
    private static final class LongOwnerTable {
        private long[] keys = new long[1024];
        private short[] owners = new short[1024];
        private int size;

        private int putIfAbsent(long key, int contextId) {
            if (key <= 0 || contextId < 0 || contextId >= 0xffff) {
                throw invalid("allocator raw ledger ID/context cannot enter the uniqueness table");
            }
            if ((size + 1L) * 10 >= keys.length * 6L) {
                resize();
            }
            int index = index(key, keys.length);
            while (keys[index] != 0) {
                if (keys[index] == key) {
                    return Short.toUnsignedInt(owners[index]) - 1;
                }
                index = (index + 1) & (keys.length - 1);
            }
            keys[index] = key;
            owners[index] = (short) (contextId + 1);
            size++;
            return -1;
        }

        private void resize() {
            if (keys.length == 1 << 25) {
                throw invalid("allocator raw allocated-ledger-ID inventory exceeds the frozen execution cap");
            }
            long[] oldKeys = keys;
            short[] oldOwners = owners;
            keys = new long[oldKeys.length << 1];
            owners = new short[oldOwners.length << 1];
            size = 0;
            for (int oldIndex = 0; oldIndex < oldKeys.length; oldIndex++) {
                if (oldKeys[oldIndex] != 0) {
                    putIfAbsent(oldKeys[oldIndex], Short.toUnsignedInt(oldOwners[oldIndex]) - 1);
                }
            }
        }

        private static int index(long value, int length) {
            long mixed = value;
            mixed ^= mixed >>> 33;
            mixed *= 0xff51afd7ed558ccdL;
            mixed ^= mixed >>> 33;
            mixed *= 0xc4ceb9fe1a85ec53L;
            mixed ^= mixed >>> 33;
            return (int) mixed & (length - 1);
        }

        private void release() {
            keys = new long[0];
            owners = new short[0];
            size = 0;
        }
    }

    private static void requireEventValueShape(AllocatorRawEvidenceEventV1 event, boolean extraCondition) {
        if (!extraCondition
                || event.value1() != 0
                || event.value2() != 0
                || event.allocatedLedgerId() != 0
                || event.ownerEpoch() != 0
                || hasOxiaFlags(event)) {
            throw invalid("allocator raw event value fields alias another metric/event meaning");
        }
    }

    private static boolean hasOxiaFlags(AllocatorRawEvidenceEventV1 event) {
        return event.oxiaOperationKind() != OxiaOperationKind.NONE || event.operationSequence() != 0;
    }

    private static boolean allZero(byte[] bytes) {
        for (byte value : bytes) {
            if (value != 0) {
                return false;
            }
        }
        return true;
    }

    private static AllocatorProtocolException invalid(String message) {
        return AllocatorSelectionReceiptV1.invalid(message);
    }

    private static AllocatorProtocolException invalid(String message, Throwable cause) {
        return new AllocatorProtocolException(AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE, message, cause);
    }
}
