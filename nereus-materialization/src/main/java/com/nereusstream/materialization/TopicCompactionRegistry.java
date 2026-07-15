/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Closed process-local registry resolving the exact decoder and strategy frozen into a durable topic policy. */
public final class TopicCompactionRegistry {
    private final Map<String, TopicCompactionDecoder> decoders;
    private final Map<StrategyKey, TopicCompactionStrategy> strategies;

    public TopicCompactionRegistry(
            List<? extends TopicCompactionDecoder> decoders,
            List<? extends TopicCompactionStrategy> strategies) {
        Objects.requireNonNull(decoders, "decoders");
        Objects.requireNonNull(strategies, "strategies");
        Map<String, TopicCompactionDecoder> decoderMap = new HashMap<>();
        for (TopicCompactionDecoder decoder : decoders) {
            TopicCompactionDecoder exact = Objects.requireNonNull(decoder, "decoders contains null");
            String id = requireText(exact.id(), "decoder id");
            if (decoderMap.putIfAbsent(id, exact) != null) {
                throw new IllegalArgumentException("duplicate topic-compaction decoder id: " + id);
            }
        }
        Map<StrategyKey, TopicCompactionStrategy> strategyMap = new HashMap<>();
        for (TopicCompactionStrategy strategy : strategies) {
            TopicCompactionStrategy exact = Objects.requireNonNull(strategy, "strategies contains null");
            StrategyKey key = new StrategyKey(
                    requireText(exact.id(), "strategy id"), exact.version());
            if (strategyMap.putIfAbsent(key, exact) != null) {
                throw new IllegalArgumentException(
                        "duplicate topic-compaction strategy identity: " + key);
            }
        }
        this.decoders = Map.copyOf(decoderMap);
        this.strategies = Map.copyOf(strategyMap);
    }

    public static TopicCompactionRegistry empty() {
        return new TopicCompactionRegistry(List.of(), List.of());
    }

    public Binding resolve(TopicCompactionSpec spec) {
        Objects.requireNonNull(spec, "spec");
        TopicCompactionDecoder decoder = decoders.get(spec.keyCodecId());
        TopicCompactionStrategy strategy = strategies.get(new StrategyKey(
                spec.strategyId(), spec.strategyVersion()));
        if (decoder == null || strategy == null) {
            throw new IllegalArgumentException(
                    "topic-compaction policy references an unregistered decoder or strategy");
        }
        // Re-read the identities at resolution so a mutable implementation cannot silently change task semantics.
        if (!decoder.id().equals(spec.keyCodecId())
                || !strategy.id().equals(spec.strategyId())
                || strategy.version() != spec.strategyVersion()) {
            throw new IllegalStateException(
                    "registered topic-compaction implementation changed its identity");
        }
        return new Binding(decoder, strategy);
    }

    public record Binding(
            TopicCompactionDecoder decoder,
            TopicCompactionStrategy strategy) {
        public Binding {
            Objects.requireNonNull(decoder, "decoder");
            Objects.requireNonNull(strategy, "strategy");
        }
    }

    private record StrategyKey(String id, long version) {
        private StrategyKey {
            requireText(id, "strategy id");
            if (version <= 0) {
                throw new IllegalArgumentException("strategy version must be positive");
            }
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
