/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import java.util.Objects;
import java.util.Optional;

/** Linearizable-by-key F2 binding lookup used to explain one historical stream incarnation. */
public record ManagedLedgerStreamProjection(
        StreamId streamId,
        Optional<VersionedVirtualLedgerProjection> streamBinding,
        Optional<VersionedTopicProjection> currentTopic) {
    public ManagedLedgerStreamProjection {
        Objects.requireNonNull(streamId, "streamId");
        streamBinding = Objects.requireNonNull(streamBinding, "streamBinding");
        currentTopic = Objects.requireNonNull(currentTopic, "currentTopic");
        if (streamBinding.isEmpty() && currentTopic.isPresent()) {
            throw new IllegalArgumentException(
                    "a topic authority cannot be resolved without the per-stream binding");
        }
        Optional<VersionedVirtualLedgerProjection> exactBinding = streamBinding;
        Optional<VersionedTopicProjection> exactTopic = currentTopic;
        exactBinding.ifPresent(binding -> {
            if (!binding.value().identity().streamId().equals(streamId.value())) {
                throw new IllegalArgumentException("stream binding identity does not match lookup stream");
            }
            exactTopic.ifPresent(topic -> {
                if (!topic.value().managedLedgerName().equals(
                        binding.value().managedLedgerName())) {
                    throw new IllegalArgumentException(
                            "current topic authority does not match the stream binding name");
                }
            });
        });
    }
}
