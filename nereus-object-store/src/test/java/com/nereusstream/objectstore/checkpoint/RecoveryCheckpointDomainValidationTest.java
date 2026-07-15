/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.StreamId;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class RecoveryCheckpointDomainValidationTest {
    @Test
    void embeddedBuffersAreCopiedAndBoundToTheirExactSha256() {
        byte[] mutable = "generation:1:".concat("a".repeat(26)).getBytes(StandardCharsets.UTF_8);
        RecoveryCheckpointPublication publication = new RecoveryCheckpointPublication(
                1,
                new PublicationId("a".repeat(26)),
                new OffsetRange(0, 1),
                ByteBuffer.wrap(mutable),
                RecoveryCheckpointTestSupport.sha256(mutable));
        mutable[0] ^= 0x01;

        assertThat(RecoveryCheckpointTestSupport.text(publication.canonicalGenerationIndexRecord()))
                .isEqualTo("generation:1:" + "a".repeat(26));
        assertThatThrownBy(() -> new RecoveryCheckpointPublication(
                        1,
                        new PublicationId("a".repeat(26)),
                        new OffsetRange(0, 1),
                        ByteBuffer.wrap(mutable),
                        RecoveryCheckpointTestSupport.sha256("different")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA256");
    }

    @Test
    void requestAndEntryRejectAmbiguousCountsAndPublicationReferences() {
        assertThatThrownBy(() -> new RecoveryCheckpointWriteRequest(
                        "cluster",
                        new StreamId("stream"),
                        1,
                        "a".repeat(26),
                        new OffsetRange(0, 2),
                        5,
                        6,
                        0,
                        2,
                        "commit-5",
                        "commit-6",
                        "commit-6",
                        6,
                        RecoveryCheckpointTestSupport.sha256("projection"),
                        1,
                        1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expectedEntryCount");

        assertThatThrownBy(() -> RecoveryCheckpointTestSupport.entry(
                        5, 0, 1, 1, "commit-5", "commit-4", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1..8");
        assertThatThrownBy(() -> RecoveryCheckpointTestSupport.entry(
                        5, 0, 1, 1, "commit-5", "commit-4", List.of(1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unique");
    }
}
