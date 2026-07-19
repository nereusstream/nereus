/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.StreamId;
import java.util.concurrent.CompletableFuture;

/** Best-effort scheduling hint that asks the existing registered-stream service to scan durable materialization truth. */
@FunctionalInterface
public interface MaterializationStreamTrigger {
    CompletableFuture<Void> trigger(StreamId streamId);
}
