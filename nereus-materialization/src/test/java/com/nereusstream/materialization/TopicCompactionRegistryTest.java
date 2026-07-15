/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class TopicCompactionRegistryTest {
    @Test
    void resolvesOnlyTheExactFrozenDecoderAndStrategyIdentity() {
        Decoder decoder = new Decoder("pulsar-key-v1");
        Strategy strategy = new Strategy("latest-key", 7);
        TopicCompactionRegistry registry = new TopicCompactionRegistry(
                List.of(decoder), List.of(strategy));

        TopicCompactionRegistry.Binding binding = registry.resolve(
                new TopicCompactionSpec("latest-key", 7, "pulsar-key-v1"));
        assertThat(binding.decoder()).isSameAs(decoder);
        assertThat(binding.strategy()).isSameAs(strategy);
        assertThatThrownBy(() -> registry.resolve(
                        new TopicCompactionSpec("latest-key", 8, "pulsar-key-v1")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> registry.resolve(
                        new TopicCompactionSpec("latest-key", 7, "unknown")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsDuplicateOrMutableRegistryIdentities() {
        assertThatThrownBy(() -> new TopicCompactionRegistry(
                        List.of(new Decoder("same"), new Decoder("same")), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TopicCompactionRegistry(
                        List.of(), List.of(new Strategy("same", 1), new Strategy("same", 1))))
                .isInstanceOf(IllegalArgumentException.class);

        MutableDecoder decoder = new MutableDecoder("decoder-v1");
        TopicCompactionRegistry registry = new TopicCompactionRegistry(
                List.of(decoder), List.of(new Strategy("latest", 1)));
        decoder.id = "decoder-v2";
        assertThatThrownBy(() -> registry.resolve(
                        new TopicCompactionSpec("latest", 1, "decoder-v1")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void compactionRecordCopiesAReadOnlyNonEmptyKeyAndUsesClosedWireIds() {
        byte[] key = new byte[] {1, 2, 3};
        CompactionRecord record = new CompactionRecord(
                11,
                ByteBuffer.wrap(key),
                CompactionDisposition.TOMBSTONE,
                OptionalLong.of(12),
                OptionalLong.empty());
        key[0] = 9;

        assertThat(bytes(record.compactionKey())).containsExactly(1, 2, 3);
        assertThat(record.compactionKey().isReadOnly()).isTrue();
        assertThat(CompactionDisposition.fromWireId(2))
                .isEqualTo(CompactionDisposition.TOMBSTONE);
        assertThatThrownBy(() -> CompactionDisposition.fromWireId(3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CompactionRecord(
                        1,
                        ByteBuffer.allocate(0),
                        CompactionDisposition.VALUE,
                        OptionalLong.empty(),
                        OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static byte[] bytes(ByteBuffer supplied) {
        ByteBuffer value = supplied.asReadOnlyBuffer();
        byte[] result = new byte[value.remaining()];
        value.get(result);
        return result;
    }

    private static class Decoder implements TopicCompactionDecoder {
        private final String id;

        private Decoder(String id) {
            this.id = id;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public Optional<CompactionRecord> decode(long offset, ByteBuffer exactPayload) {
            return Optional.empty();
        }
    }

    private static final class MutableDecoder implements TopicCompactionDecoder {
        private String id;

        private MutableDecoder(String id) {
            this.id = id;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public Optional<CompactionRecord> decode(long offset, ByteBuffer exactPayload) {
            return Optional.empty();
        }
    }

    private record Strategy(String id, long version) implements TopicCompactionStrategy {
        @Override
        public boolean retainTombstone(CompactionRecord tombstone, long planningTimeMillis) {
            return true;
        }
    }
}
