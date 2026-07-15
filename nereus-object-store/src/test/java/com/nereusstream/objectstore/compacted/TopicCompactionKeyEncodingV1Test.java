/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class TopicCompactionKeyEncodingV1Test {
    @Test
    void keyedAndUnkeyedNamespacesCannotCollide() {
        ByteBuffer keyed = TopicCompactionKeyEncodingV1.keyed(
                ByteBuffer.wrap(new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 7}));
        ByteBuffer unkeyed = TopicCompactionKeyEncodingV1.unkeyed(7);

        assertThat(bytes(keyed)).isNotEqualTo(bytes(unkeyed));
        assertThat(TopicCompactionKeyEncodingV1.decode(keyed))
                .isInstanceOf(TopicCompactionKeyEncodingV1.DecodedKey.Keyed.class);
        assertThat(TopicCompactionKeyEncodingV1.decode(unkeyed))
                .isEqualTo(new TopicCompactionKeyEncodingV1.DecodedKey.Unkeyed(7));
        TopicCompactionKeyEncodingV1.validateForOffset(unkeyed, 7);
    }

    @Test
    void rejectsMalformedTagsAndUnkeyedOffsetMismatch() {
        assertThatThrownBy(() -> TopicCompactionKeyEncodingV1.decode(
                        ByteBuffer.wrap(new byte[] {2, 1})))
                .isInstanceOf(CompactedObjectFormatException.class);
        assertThatThrownBy(() -> TopicCompactionKeyEncodingV1.validateForOffset(
                        TopicCompactionKeyEncodingV1.unkeyed(8), 7))
                .isInstanceOf(CompactedObjectFormatException.class);
    }

    private static byte[] bytes(ByteBuffer value) {
        ByteBuffer source = value.asReadOnlyBuffer();
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        return bytes;
    }
}
