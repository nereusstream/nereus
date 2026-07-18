/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.StreamId;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Plans and revalidates an ephemeral logical-retention candidate from durable truth. */
public interface RetentionCandidatePlanner {
    CompletableFuture<Optional<RetentionCandidate>> plan(
            StreamId streamId,
            RetentionPolicySnapshot policy);

    CompletableFuture<Void> revalidate(
            RetentionCandidate candidate,
            RetentionPolicySnapshot policy);
}
