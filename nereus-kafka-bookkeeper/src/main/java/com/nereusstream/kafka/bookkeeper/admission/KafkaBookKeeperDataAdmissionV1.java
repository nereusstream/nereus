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

package com.nereusstream.kafka.bookkeeper.admission;

import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import java.util.Objects;

/** Immutable intersection of persisted, provider, and Kafka-native DATA limits. */
public final class KafkaBookKeeperDataAdmissionV1 {
    private final Nbke2RunBindingV1 runBinding;
    private final BookKeeperCapabilitySnapshotV1 providerCapability;
    private final int admittedKafkaCompleteRecordBatchBytes;
    private final int nonTerminalDataOverheadBytes;
    private final int terminalDataOverheadBytes;
    private final int effectiveMaxDataFrameBytes;
    private final int maximumAdmittedRawRecordBatchBytes;

    private KafkaBookKeeperDataAdmissionV1(
            Nbke2RunBindingV1 runBinding,
            BookKeeperCapabilitySnapshotV1 providerCapability,
            int admittedKafkaCompleteRecordBatchBytes,
            int nonTerminalDataOverheadBytes,
            int terminalDataOverheadBytes,
            int effectiveMaxDataFrameBytes,
            int maximumAdmittedRawRecordBatchBytes) {
        this.runBinding = runBinding;
        this.providerCapability = providerCapability;
        this.admittedKafkaCompleteRecordBatchBytes = admittedKafkaCompleteRecordBatchBytes;
        this.nonTerminalDataOverheadBytes = nonTerminalDataOverheadBytes;
        this.terminalDataOverheadBytes = terminalDataOverheadBytes;
        this.effectiveMaxDataFrameBytes = effectiveMaxDataFrameBytes;
        this.maximumAdmittedRawRecordBatchBytes = maximumAdmittedRawRecordBatchBytes;
    }

    public static KafkaBookKeeperDataAdmissionV1 admitProfile(
            Nbke2RunBindingV1 runBinding,
            BookKeeperCapabilitySnapshotV1 providerCapability,
            int admittedKafkaCompleteRecordBatchBytes) {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(providerCapability, "providerCapability");
        if (!runBinding.providerScopeId().equals(providerCapability.providerScopeId())) {
            throw new IllegalArgumentException("run and provider capability scopes differ");
        }
        if (admittedKafkaCompleteRecordBatchBytes <= 0) {
            throw new IllegalArgumentException("Kafka complete RecordBatch limit must be positive");
        }

        int nonTerminalOverhead = Nbke2CodecV1.dataFrameOverheadBytes(runBinding, false);
        int terminalOverhead = Nbke2CodecV1.dataFrameOverheadBytes(runBinding, true);
        try {
            long kafkaLimitedFrame = Math.addExact((long) admittedKafkaCompleteRecordBatchBytes, terminalOverhead);
            long effectiveFrame = Math.min(
                    Math.min(
                            (long) Nbke2ConstantsV1.FORMAT_MAX_FRAME_BYTES,
                            providerCapability.maximumAddPayloadBytes()),
                    kafkaLimitedFrame);
            long rawLimit = Math.min(
                    (long) Nbke2ConstantsV1.FORMAT_MAX_DATA_PAYLOAD_BYTES,
                    Math.subtractExact(effectiveFrame, terminalOverhead));
            if (rawLimit <= 0) {
                throw new IllegalArgumentException(
                        "admitted provider/Kafka profile cannot carry one terminal DATA byte");
            }
            return new KafkaBookKeeperDataAdmissionV1(
                    runBinding,
                    providerCapability,
                    admittedKafkaCompleteRecordBatchBytes,
                    nonTerminalOverhead,
                    terminalOverhead,
                    Math.toIntExact(effectiveFrame),
                    Math.toIntExact(rawLimit));
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("DATA admission profile arithmetic overflows", failure);
        }
    }

    public KafkaBookKeeperDataAdmissionTicketV1 admitBeforeOffsetAllocation(
            long rawRecordBatchBytes, long memberOrdinal, long memberCount) {
        if (rawRecordBatchBytes <= 0 || rawRecordBatchBytes > Integer.MAX_VALUE) {
            throw reject(
                    KafkaBookKeeperAdmissionRejectionV1.INVALID_RECORD_BATCH_LENGTH,
                    "raw RecordBatch length is outside the allocation domain");
        }
        if (memberCount <= 0 || memberCount > Integer.MAX_VALUE || memberOrdinal < 0 || memberOrdinal >= memberCount) {
            throw reject(
                    KafkaBookKeeperAdmissionRejectionV1.INVALID_APPEND_GROUP_POSITION,
                    "append-group ordinal/count are outside the v1 domain");
        }

        boolean terminal = memberOrdinal == memberCount - 1;
        int overhead = terminal ? terminalDataOverheadBytes : nonTerminalDataOverheadBytes;
        long encodedBytes;
        long terminalSizedFrameBytes;
        try {
            encodedBytes = Math.addExact(rawRecordBatchBytes, overhead);
            terminalSizedFrameBytes = Math.addExact(rawRecordBatchBytes, terminalDataOverheadBytes);
        } catch (ArithmeticException failure) {
            throw new KafkaBookKeeperAdmissionException(
                    KafkaBookKeeperAdmissionRejectionV1.ARITHMETIC_OVERFLOW,
                    "encoded DATA frame length overflows",
                    failure);
        }
        if (rawRecordBatchBytes > Nbke2ConstantsV1.FORMAT_MAX_DATA_PAYLOAD_BYTES
                || terminalSizedFrameBytes > Nbke2ConstantsV1.FORMAT_MAX_FRAME_BYTES) {
            throw reject(
                    KafkaBookKeeperAdmissionRejectionV1.PERSISTED_FORMAT_LIMIT_EXCEEDED,
                    "DATA exceeds the persisted NBKE2 v1 format");
        }
        if (terminalSizedFrameBytes > providerCapability.maximumAddPayloadBytes()) {
            throw reject(
                    KafkaBookKeeperAdmissionRejectionV1.PROVIDER_CAPABILITY_LIMIT_EXCEEDED,
                    "DATA exceeds the admitted BookKeeper add-payload capability");
        }
        if (rawRecordBatchBytes > admittedKafkaCompleteRecordBatchBytes) {
            throw reject(
                    KafkaBookKeeperAdmissionRejectionV1.KAFKA_NATIVE_LIMIT_EXCEEDED,
                    "DATA exceeds the admitted Kafka complete RecordBatch limit");
        }
        return new KafkaBookKeeperDataAdmissionTicketV1(
                runBinding.providerScopeId(),
                Math.toIntExact(rawRecordBatchBytes),
                Math.toIntExact(encodedBytes),
                Math.toIntExact(memberOrdinal),
                Math.toIntExact(memberCount),
                terminal);
    }

    public Nbke2RunBindingV1 runBinding() {
        return runBinding;
    }

    public BookKeeperCapabilitySnapshotV1 providerCapability() {
        return providerCapability;
    }

    public int admittedKafkaCompleteRecordBatchBytes() {
        return admittedKafkaCompleteRecordBatchBytes;
    }

    public int nonTerminalDataOverheadBytes() {
        return nonTerminalDataOverheadBytes;
    }

    public int terminalDataOverheadBytes() {
        return terminalDataOverheadBytes;
    }

    public int effectiveMaxDataFrameBytes() {
        return effectiveMaxDataFrameBytes;
    }

    public int maximumAdmittedRawRecordBatchBytes() {
        return maximumAdmittedRawRecordBatchBytes;
    }

    private static KafkaBookKeeperAdmissionException reject(
            KafkaBookKeeperAdmissionRejectionV1 rejection, String message) {
        return new KafkaBookKeeperAdmissionException(rejection, message);
    }
}
