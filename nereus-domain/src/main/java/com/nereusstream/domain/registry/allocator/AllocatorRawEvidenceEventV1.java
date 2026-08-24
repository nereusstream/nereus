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

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.nio.ByteBuffer;

/**
 * One fixed 64-byte NARE1 union event. Multiple independent events belong to one request ordinal.
 *
 * <p>The first 56 bytes are fields and the final eight bytes are canonical zero. Flags use low bits {@code 0..3} for
 * one ADR-0094 fault cut, {@code 4..7} for the Oxia operation kind, {@code 8..12} for a per-request operation
 * sequence, and bit {@code 15} for warm-up. Bits {@code 13..14} are zero. Measurement events have no fault-cut bits;
 * only Oxia and exact write-proof events carry an operation kind/sequence.
 *
 * <p>Lifecycle and append endpoints carry zero {@code value1/value2/allocatedLedgerId/ownerEpoch}; terminal lifecycle
 * events additionally carry their matching typed outcome. {@link EventKind#QUEUE_DEPTH} uses {@code value1=depth}
 * and {@code value2=QUEUE_ENQUEUE|QUEUE_DEQUEUE}. Oxia START/END use {@code value1=request/response bytes} and the same
 * non-zero operation token in {@code value2}. {@link EventKind#ALLOCATED_LEDGER_ID} carries only the allocated ID (and
 * a positive owner epoch for candidates). GRANT_USE carries {@code value1=grantId}, the used ID, and owner epoch;
 * GRANT_WASTE carries {@code value1=grantId}, {@code value2=wasted IDs}, and owner epoch. STALE_CANDIDATE_BURN carries
 * grant ID, burned ID, and fresh owner epoch; PERMANENT_ORPHAN carries only the orphan ID. Fault write proof uses the
 * same non-zero write token in {@code value1}, canonical mutation byte count in {@code value2}, exact operation
 * kind/sequence, and positive dispatch-owner epoch across dispatch/reread/typed-terminal events. OWNER loss identifies
 * the lost actor, while every fresh-owner append identifies one affected ledger and a positive fresh owner epoch.
 */
public record AllocatorRawEvidenceEventV1(
        AllocatorEvidenceContextV1 context,
        EventKind kind,
        int actorId,
        TriggerKind trigger,
        EventOutcome outcome,
        int flags,
        long requestOrdinal,
        long managedLedgerIndex,
        long monotonicTimestampMicros,
        long value1,
        long value2,
        long allocatedLedgerId,
        long ownerEpoch) {
    public static final int BYTES = 64;
    public static final int FLAG_FAULT_CUT_MASK = 0x000f;
    public static final int FLAG_OXIA_OPERATION_KIND_MASK = 0x00f0;
    public static final int FLAG_OXIA_OPERATION_SEQUENCE_MASK = 0x1f00;
    public static final int FLAG_RESERVED_MASK = 0x6000;
    public static final int FLAG_WARMUP = 1 << 15;
    public static final long QUEUE_ENQUEUE = 1;
    public static final long QUEUE_DEQUEUE = 2;

    public AllocatorRawEvidenceEventV1 {
        if (context == null
                || kind == null
                || trigger == null
                || outcome == null
                || actorId < 0
                || actorId >= 4
                || flags < 0
                || flags > 0xffff
                || (flags & FLAG_RESERVED_MASK) != 0
                || requestOrdinal < 0
                || requestOrdinal > 0xffff_ffffL
                || managedLedgerIndex < 0
                || managedLedgerIndex >= context.activeManagedLedgers()
                || monotonicTimestampMicros < 0
                || value1 < 0
                || value2 < 0
                || allocatedLedgerId < 0
                || ownerEpoch < 0) {
            throw new IllegalArgumentException("allocator raw evidence event field is outside its fixed bounds");
        }
    }

    public CanonicalBytes encode() {
        ByteBuffer output = ByteBuffer.allocate(BYTES);
        output.putShort((short) context.contextId())
                .put((byte) kind.code)
                .put((byte) actorId)
                .put((byte) trigger.code)
                .put((byte) outcome.code)
                .putShort((short) flags)
                .putInt((int) requestOrdinal)
                .putInt((int) managedLedgerIndex)
                .putLong(monotonicTimestampMicros)
                .putLong(value1)
                .putLong(value2)
                .putLong(allocatedLedgerId)
                .putLong(ownerEpoch)
                .putLong(0);
        return CanonicalBytes.copyOf(output.array());
    }

    static AllocatorRawEvidenceEventV1 decode(ByteBuffer input) {
        if (input.remaining() != BYTES) {
            throw AllocatorSelectionReceiptV1.invalid("allocator raw event length differs");
        }
        AllocatorRawEvidenceEventV1 decoded = new AllocatorRawEvidenceEventV1(
                AllocatorEvidenceContextV1.fromId(Short.toUnsignedInt(input.getShort())),
                EventKind.fromCode(Byte.toUnsignedInt(input.get())),
                Byte.toUnsignedInt(input.get()),
                TriggerKind.fromCode(Byte.toUnsignedInt(input.get())),
                EventOutcome.fromCode(Byte.toUnsignedInt(input.get())),
                Short.toUnsignedInt(input.getShort()),
                Integer.toUnsignedLong(input.getInt()),
                Integer.toUnsignedLong(input.getInt()),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong());
        if (input.getLong() != 0 || input.hasRemaining()) {
            throw AllocatorSelectionReceiptV1.invalid("allocator raw event reserved bytes are non-zero");
        }
        return decoded;
    }

    public static int flags(
            AllocatorFaultCutV1 faultCut, OxiaOperationKind oxiaOperationKind, int operationSequence, boolean warmup) {
        if (operationSequence < 0 || operationSequence > 31) {
            throw new IllegalArgumentException("Oxia operation sequence must be in [0,31]");
        }
        int cut = faultCut == null ? 0 : faultCut.code();
        int operation = oxiaOperationKind == null ? 0 : oxiaOperationKind.code;
        return cut | (operation << 4) | (operationSequence << 8) | (warmup ? FLAG_WARMUP : 0);
    }

    public int operationSequence() {
        return (flags & FLAG_OXIA_OPERATION_SEQUENCE_MASK) >>> 8;
    }

    public OxiaOperationKind oxiaOperationKind() {
        return OxiaOperationKind.fromCode((flags & FLAG_OXIA_OPERATION_KIND_MASK) >>> 4);
    }

    public enum EventKind {
        OFFERED(1),
        ENQUEUED(2),
        DISPATCHED(3),
        ADMITTED(4),
        COMPLETED(5),
        FENCED(6),
        FAILED(7),
        TIMED_OUT(8),
        APPEND_ADMISSION_START(9),
        APPEND_ADMISSION_RELEASE(10),
        OXIA_OPERATION_START(11),
        OXIA_OPERATION_END(12),
        QUEUE_DEPTH(13),
        OWNER_LOSS_DETECTED(14),
        FRESH_OWNER_APPEND_COMPLETE(15),
        GRANT_USE(16),
        GRANT_WASTE(17),
        STALE_CANDIDATE_BURN(18),
        PERMANENT_ORPHAN(19),
        ALLOCATED_LEDGER_ID(20),
        METADATA_WRITE_DISPATCH(21),
        SAME_KEY_REREAD(22),
        TYPED_TERMINAL_DISPOSITION(23),
        CUT_BEGIN(24),
        CUT_END(25),
        ASSERTION_FAILED(26),
        TEST_SKIPPED(27),
        UNEXPECTED_ERROR(28);

        private final int code;

        EventKind(int code) {
            this.code = code;
        }

        static EventKind fromCode(int code) {
            for (EventKind value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            throw AllocatorSelectionReceiptV1.invalid("allocator raw event kind is unknown");
        }
    }

    public enum TriggerKind {
        ENTRY(1),
        BYTE(2),
        AGE(3);

        private final int code;

        TriggerKind(int code) {
            this.code = code;
        }

        static TriggerKind fromCode(int code) {
            for (TriggerKind value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            throw AllocatorSelectionReceiptV1.invalid("allocator raw event trigger is unknown");
        }
    }

    public enum EventOutcome {
        NONE(0),
        SUCCESS(1),
        FENCED(2),
        FAILED(3),
        TIMED_OUT(4),
        APPLIED_EXACT(5),
        PREDECESSOR_UNCHANGED(6),
        CONFLICT(7);

        private final int code;

        EventOutcome(int code) {
            this.code = code;
        }

        static EventOutcome fromCode(int code) {
            for (EventOutcome value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            throw AllocatorSelectionReceiptV1.invalid("allocator raw event outcome is unknown");
        }
    }

    public enum OxiaOperationKind {
        NONE(0),
        CELL_CREATE(1),
        CELL_RESERVE_CAS(2),
        HEAD_CREATE(3),
        RANGE_GRANT_INSTALL_CAS(4),
        NODE_CREATE(5),
        HEAD_PUBLISH_CAS(6),
        CELL_CLEAR_CAS(7),
        HEAD_TAKEOVER_CAS(8),
        HEAD_STALE_BURN_CAS(9),
        EXACT_READ(10);

        private final int code;

        OxiaOperationKind(int code) {
            this.code = code;
        }

        static OxiaOperationKind fromCode(int code) {
            for (OxiaOperationKind value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            throw AllocatorSelectionReceiptV1.invalid("allocator raw event Oxia operation kind is unknown");
        }
    }
}
