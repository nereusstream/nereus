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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperTimeoutClassV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperDataAdmissionV1Test {
    @Test
    void freezesTheCheckedMinimumFormulaAndExactTerminalOverhead() {
        KafkaBookKeeperDataAdmissionV1 admission = admission(4_000_000, 3_000_000);

        assertThat(admission.nonTerminalDataOverheadBytes()).isEqualTo(244);
        assertThat(admission.terminalDataOverheadBytes()).isEqualTo(308);
        assertThat(admission.effectiveMaxDataFrameBytes()).isEqualTo(3_000_308);
        assertThat(admission.maximumAdmittedRawRecordBatchBytes()).isEqualTo(3_000_000);

        KafkaBookKeeperDataAdmissionTicketV1 nonTerminal = admission.admitBeforeOffsetAllocation(1_000, 0, 2);
        KafkaBookKeeperDataAdmissionTicketV1 terminal = admission.admitBeforeOffsetAllocation(1_000, 1, 2);
        assertThat(nonTerminal.encodedDataFrameBytes()).isEqualTo(1_244);
        assertThat(nonTerminal.terminalMember()).isFalse();
        assertThat(terminal.encodedDataFrameBytes()).isEqualTo(1_308);
        assertThat(terminal.terminalMember()).isTrue();
    }

    @Test
    void provesOneBeforeAtAndOneAfterThePersistedRawFormatCap() {
        KafkaBookKeeperDataAdmissionV1 admission = admission(9_000_000, 9_000_000);
        int cap = Nbke2ConstantsV1.FORMAT_MAX_DATA_PAYLOAD_BYTES;

        assertThat(admission.admitBeforeOffsetAllocation(cap - 1L, 0, 1).rawRecordBatchBytes())
                .isEqualTo(cap - 1);
        assertThat(admission.admitBeforeOffsetAllocation(cap, 0, 1).rawRecordBatchBytes())
                .isEqualTo(cap);
        assertRejected(admission, cap + 1L, KafkaBookKeeperAdmissionRejectionV1.PERSISTED_FORMAT_LIMIT_EXCEEDED);
    }

    @Test
    void provesOneBeforeAtAndOneAfterTheProviderPayloadCap() {
        KafkaBookKeeperDataAdmissionV1 admission = admission(1_000, 9_000_000);
        int cap = 1_000 - admission.terminalDataOverheadBytes();

        assertThat(admission.admitBeforeOffsetAllocation(cap - 1L, 0, 2).rawRecordBatchBytes())
                .isEqualTo(cap - 1);
        assertThat(admission.admitBeforeOffsetAllocation(cap, 0, 2).rawRecordBatchBytes())
                .isEqualTo(cap);
        assertRejected(admission, cap + 1L, KafkaBookKeeperAdmissionRejectionV1.PROVIDER_CAPABILITY_LIMIT_EXCEEDED);
    }

    @Test
    void provesOneBeforeAtAndOneAfterTheKafkaNativeBatchCap() {
        KafkaBookKeeperDataAdmissionV1 admission = admission(4_000_000, 1_000);

        assertThat(admission.admitBeforeOffsetAllocation(999, 0, 1).rawRecordBatchBytes())
                .isEqualTo(999);
        assertThat(admission.admitBeforeOffsetAllocation(1_000, 0, 1).rawRecordBatchBytes())
                .isEqualTo(1_000);
        assertRejected(admission, 1_001, KafkaBookKeeperAdmissionRejectionV1.KAFKA_NATIVE_LIMIT_EXCEEDED);
    }

    @Test
    void rejectsInvalidProfileScopeEmptyBatchAndAppendGroupDomainBeforeTicketCreation() {
        BookKeeperCapabilitySnapshotV1 otherScope = capability(new CellProviderScopeId(digest(9)), 4_000_000);
        assertThatThrownBy(() -> KafkaBookKeeperDataAdmissionV1.admitProfile(binding(), otherScope, 1_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scopes differ");
        assertThatThrownBy(() -> admission(250, 1_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot carry one terminal DATA byte");

        KafkaBookKeeperDataAdmissionV1 admission = admission(4_000_000, 1_000);
        assertRejected(admission, 0, KafkaBookKeeperAdmissionRejectionV1.INVALID_RECORD_BATCH_LENGTH);
        assertThatThrownBy(() -> admission.admitBeforeOffsetAllocation(1, 1, 1))
                .isInstanceOfSatisfying(
                        KafkaBookKeeperAdmissionException.class, rejection -> assertThat(rejection.rejection())
                                .isEqualTo(KafkaBookKeeperAdmissionRejectionV1.INVALID_APPEND_GROUP_POSITION));
        assertThatThrownBy(() -> admission.admitBeforeOffsetAllocation(1, 0, (long) Integer.MAX_VALUE + 1L))
                .isInstanceOfSatisfying(
                        KafkaBookKeeperAdmissionException.class, rejection -> assertThat(rejection.rejection())
                                .isEqualTo(KafkaBookKeeperAdmissionRejectionV1.INVALID_APPEND_GROUP_POSITION));
    }

    private static KafkaBookKeeperDataAdmissionV1 admission(int maximumAddPayloadBytes, int kafkaBatchBytes) {
        Nbke2RunBindingV1 binding = binding();
        return KafkaBookKeeperDataAdmissionV1.admitProfile(
                binding, capability(binding.providerScopeId(), maximumAddPayloadBytes), kafkaBatchBytes);
    }

    private static Nbke2RunBindingV1 binding() {
        return new Nbke2RunBindingV1(
                new TopicBindingId(digest(1)),
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(0, 2)), new KafkaTopicName("orders")),
                7,
                new StorageEpochId(digest(3)),
                11,
                5,
                new CellProviderScopeId(digest(4)),
                new StorageRunId(new Id128(0, 6)));
    }

    private static BookKeeperCapabilitySnapshotV1 capability(
            CellProviderScopeId providerScopeId, int maximumAddPayloadBytes) {
        int frameLimit = Math.max(maximumAddPayloadBytes + 1, 10_000_000);
        return new BookKeeperCapabilitySnapshotV1(
                providerScopeId,
                "cd06340851d6d657b7c7546df01df365c18980de",
                digest(5),
                "cd06340851d6d657b7c7546df01df365c18980de",
                digest(6),
                BookKeeperProtocolModeV1.V3,
                frameLimit,
                frameLimit,
                maximumAddPayloadBytes,
                true,
                3,
                3,
                2,
                BookKeeperDigestTypeV1.CRC32C,
                true,
                true,
                new BookKeeperTimeoutClassV1(1_000, 2_000, 2_000, 5_000),
                "bk-credential:v7",
                digest(7));
    }

    private static Sha256Digest digest(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return Sha256Digest.copyOf(bytes);
    }

    private static void assertRejected(
            KafkaBookKeeperDataAdmissionV1 admission,
            long rawRecordBatchBytes,
            KafkaBookKeeperAdmissionRejectionV1 expected) {
        assertThatThrownBy(() -> admission.admitBeforeOffsetAllocation(rawRecordBatchBytes, 0, 1))
                .isInstanceOfSatisfying(
                        KafkaBookKeeperAdmissionException.class,
                        rejection -> assertThat(rejection.rejection()).isEqualTo(expected));
    }
}
