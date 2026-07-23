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

package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.objectstore.Crc32cChecksums;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RangedLosslessMaterializationRowPublisherTest {
    @Test
    void mapsOneExactKafkaBatchToOneDenseNcp2Row() {
        byte[] payload = "kafka-batch".getBytes(StandardCharsets.UTF_8);
        ReadBatch batch = batch(new OffsetRange(10, 13), PayloadFormat.KAFKA_RECORD_BATCH, payload);

        var row = RangedLosslessMaterializationRowPublisher.row(batch, 7);

        assertThat(row.streamOffsetStart()).isEqualTo(10);
        assertThat(row.recordCount()).isEqualTo(3);
        assertThat(row.entryOrdinal()).isEqualTo(7);
        byte[] returned = new byte[row.exactPayload().remaining()];
        row.exactPayload().get(returned);
        assertThat(returned).containsExactly(payload);
        assertThat(row.payloadCrc32c())
                .isEqualTo(Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)));
    }

    @Test
    void rejectsNonKafkaPayloadWithoutReinterpretingIt() {
        ReadBatch opaque = batch(
                new OffsetRange(10, 11),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                new byte[] {1});

        assertThatThrownBy(() -> RangedLosslessMaterializationRowPublisher.row(opaque, 0))
                .isInstanceOfSatisfying(
                        NereusException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ErrorCode.UNSUPPORTED_FORMAT));
    }

    private static ReadBatch batch(OffsetRange range, PayloadFormat format, byte[] payload) {
        BookKeeperEntryRangeReadTarget target = new BookKeeperEntryRangeReadTarget(
                1,
                "bk",
                1,
                0,
                1,
                BookKeeperEntryMapping.RANGED_NEREUS_ENTRY_V1,
                new Checksum(ChecksumType.SHA256, "1".repeat(64)));
        ReadSourceRef source = new ReadSourceRef(
                range,
                0,
                1,
                target,
                ReadTargetIdentities.sha256(target));
        return new ReadBatch(range, format, payload, List.of(), Optional.empty(), source);
    }
}
