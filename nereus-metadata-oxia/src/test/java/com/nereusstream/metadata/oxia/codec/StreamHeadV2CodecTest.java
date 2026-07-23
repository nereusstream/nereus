/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord;
import com.nereusstream.metadata.oxia.records.StreamHeadRecord;
import java.io.IOException;
import java.io.InputStream;
import java.util.HexFormat;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class StreamHeadV2CodecTest {
    @Test
    void legacyGoldenRemainsByteExactAndDecodesWithEmptyAuthority() throws IOException {
        Properties goldens = new Properties();
        try (InputStream input = getClass().getResourceAsStream("phase1-metadata-codec-golden.properties")) {
            goldens.load(input);
        }
        byte[] frozen = HexFormat.of().parseHex(goldens.getProperty("StreamHeadRecord"));

        StreamHeadRecord decoded = Phase1MetadataCodecs.decodeEnvelope(frozen, StreamHeadRecord.class);

        assertThat(decoded.appendSession().hasAuthority()).isFalse();
        assertThat(Phase1MetadataCodecs.encodeEnvelope(decoded, StreamHeadRecord.class)).isEqualTo(frozen);
    }

    @Test
    void authorityRecordsUseV2EnvelopeAndRoundTripExactly() {
        AppendSessionSnapshotRecord session = new AppendSessionSnapshotRecord(
                "writer", 4, "token", 7, 9_000,
                "kafka-partition-leader-v1", "cluster/topic/3", 12, "42", 99);
        StreamHeadRecord head = new StreamHeadRecord(
                "stream", "kafka/cluster/topic/3/incarnation-1", "name-hash",
                "ACTIVE", "OBJECT_WAL_SYNC_OBJECT", Map.of("z", "9", "a", "1"),
                1_000, 2, 100, 8_000, 6, 10, "commit", session, 13);

        byte[] encoded = Phase1MetadataCodecs.encodeEnvelope(head, StreamHeadRecord.class);
        MetadataRecordEnvelope.DecodedEnvelope envelope = MetadataRecordEnvelope.decode(encoded);

        assertThat(envelope.schemaVersion()).isEqualTo(2);
        assertThat(envelope.minReaderSchemaVersion()).isEqualTo(2);
        assertThat(Phase1MetadataCodecs.decodeEnvelope(encoded, StreamHeadRecord.class)).isEqualTo(head);

        AppendSessionRecord standalone = AppendSessionRecord.fromHead(head.streamId(), session);
        byte[] standaloneBytes = Phase1MetadataCodecs.encodeEnvelope(standalone, AppendSessionRecord.class);
        assertThat(MetadataRecordEnvelope.decode(standaloneBytes).schemaVersion()).isEqualTo(2);
        assertThat(Phase1MetadataCodecs.decodeEnvelope(standaloneBytes, AppendSessionRecord.class))
                .isEqualTo(standalone);
    }

    @Test
    void rejectsV2PayloadPresentedAsLegacyEnvelope() {
        StreamHeadRecord head = authorityHead();
        MetadataRecordEnvelope.DecodedEnvelope v2 = MetadataRecordEnvelope.decode(
                Phase1MetadataCodecs.encodeEnvelope(head, StreamHeadRecord.class));
        byte[] falselyLegacy = MetadataRecordEnvelope.encode(
                v2.recordType(), 1, 1, v2.payloadEncoding(), v2.payload());

        assertThatThrownBy(() -> Phase1MetadataCodecs.decodeEnvelope(falselyLegacy, StreamHeadRecord.class))
                .isInstanceOf(MetadataCodecException.class)
                .hasMessageContaining("payload schema");
    }

    private static StreamHeadRecord authorityHead() {
        return new StreamHeadRecord(
                "stream", "kafka/cluster/topic/3/incarnation-1", "name-hash",
                "ACTIVE", "OBJECT_WAL_SYNC_OBJECT", Map.of(), 1_000, 1,
                0, 0, 0, 0, "",
                new AppendSessionSnapshotRecord(
                        "writer", 1, "token", 1, 2_000,
                        "kafka-partition-leader-v1", "cluster/topic/3", 1, "1", 1),
                1);
    }
}
