/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.kafka.compaction;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.read.GenerationReadConstraint;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import java.util.concurrent.CompletableFuture;

/**
 * Resolves one binding-rooted Kafka compaction activation into exact durable generation identities.
 */
@FunctionalInterface
public interface KafkaActivatedGenerationAuthority {
  CompletableFuture<GenerationReadConstraint> resolve(
      StreamId streamId, KafkaCompactionCoverageRecord coverage);

  static KafkaActivatedGenerationAuthority unavailable() {
    return (streamId, coverage) ->
        CompletableFuture.failedFuture(
            new NereusException(
                ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                false,
                "activated Kafka generation discovery is not configured"));
  }
}
