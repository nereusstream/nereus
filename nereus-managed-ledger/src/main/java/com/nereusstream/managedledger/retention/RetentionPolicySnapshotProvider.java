/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.StreamId;
import java.util.concurrent.CompletableFuture;

/** Reloads the exact effective Pulsar topic-retention policy and its authority version. */
@FunctionalInterface
public interface RetentionPolicySnapshotProvider {
    CompletableFuture<RetentionPolicySnapshot> snapshot(StreamId streamId);
}
