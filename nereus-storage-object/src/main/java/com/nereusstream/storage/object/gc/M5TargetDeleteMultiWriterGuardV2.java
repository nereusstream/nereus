/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Sorted physical tickets around one concrete writer. This is not a cross-key transaction or native owner verifier. */
public final class M5TargetDeleteMultiWriterGuardV2 {
    public static final int MAX_TARGETS = 2048;
    public static final int MAX_TARGET_BYTES = 1_048_576;
    public static final int MAX_RECOVERY_TICKETS = 256;

    public record Context(
            ProofBoundWriterClassV1 writerClass,
            Sha256Digest capabilitySha256,
            Sha256Digest ownerFenceSha256,
            Sha256Digest externalFactsRootSha256) {
        public Context {
            Objects.requireNonNull(writerClass, "writerClass");
            M5TargetDeleteAuthorityRecordsV1.requireDigest(capabilitySha256, "capabilitySha256");
            M5TargetDeleteAuthorityRecordsV1.requireDigest(ownerFenceSha256, "ownerFenceSha256");
            M5TargetDeleteAuthorityRecordsV1.requireDigest(externalFactsRootSha256, "externalFactsRootSha256");
        }

        boolean matches(ProofBoundWriterTicketV1 ticket) {
            return ticket.writerClass() == writerClass
                    && ticket.capabilitySha256().equals(capabilitySha256)
                    && ticket.ownerFenceSha256().equals(ownerFenceSha256)
                    && ticket.externalFactsRootSha256().equals(externalFactsRootSha256);
        }
    }

    /** The concrete writer must supply exact native terminal reconciliation; a local return is insufficient. */
    public record Completion<T>(T value, Optional<Sha256Digest> terminalProof) {
        public Completion {
            Objects.requireNonNull(value, "value");
            terminalProof = Objects.requireNonNull(terminalProof, "terminalProof");
            terminalProof.ifPresent(
                    valueSha -> M5TargetDeleteAuthorityRecordsV1.requireDigest(valueSha, "terminalProof"));
        }
    }

    public record Result<T>(
            Sha256Digest operationId,
            boolean mutationInvoked,
            Optional<T> value,
            List<PhysicalResourceIdV2> unresolvedTargets) {
        public Result {
            Objects.requireNonNull(operationId, "operationId");
            value = Objects.requireNonNull(value, "value");
            unresolvedTargets = List.copyOf(unresolvedTargets);
        }
    }

    private record Attempt(PhysicalResourceIdV2 resource, ProofBoundWriterTicketV1 ticket) {}

    private final M5TargetDeleteAuthorityCoordinatorV1 coordinator;

    public M5TargetDeleteMultiWriterGuardV2(M5TargetDeleteAuthorityCoordinatorV1 coordinator) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    }

    /** No absent authority is created; initialization and quota reservation belong to resource admission. */
    public <T> CompletionStage<Result<T>> execute(
            List<? extends PhysicalResourceIdV2> resources,
            Context context,
            Supplier<? extends CompletionStage<Completion<T>>> mutation) {
        var targets = canonicalTargets(resources);
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(mutation, "mutation");
        // Every invocation has distinct tickets, even if two callers retry the same logical publication concurrently.
        // No public method resumes external dispatch with this nonce; recovery can only reconcile a native terminal.
        var nonce = UUID.randomUUID();
        var operation = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(48)
                .put(context.externalFactsRootSha256().bytes().toByteArray())
                .putLong(nonce.getMostSignificantBits())
                .putLong(nonce.getLeastSignificantBits())
                .array()));
        var attempted = new ArrayList<Attempt>();
        CompletionStage<Boolean> acquired = CompletableFuture.completedFuture(true);
        for (var resource : targets) {
            acquired = acquired.thenCompose(previous -> previous
                    ? acquire(resource, context, operation, attempted)
                    : CompletableFuture.completedFuture(false));
        }
        var work = acquired.thenCompose(complete -> {
            if (!complete) {
                // The callback has never been invoked and cannot be invoked by this execution after this branch.
                var noDispatch = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(36)
                        .putInt(0x4d354e44)
                        .put(operation.bytes().toByteArray())
                        .array()));
                return release(attempted, noDispatch)
                        .thenApply(left -> new Result<T>(operation, false, Optional.empty(), left));
            }
            CompletionStage<Completion<T>> external;
            try {
                external = Objects.requireNonNull(mutation.get(), "writer returned no completion stage");
            } catch (Throwable failure) {
                return CompletableFuture.completedFuture(new Result<T>(operation, true, Optional.empty(), targets));
            }
            return external.handle((result, failure) -> failure == null ? result : null)
                    .thenCompose(result -> {
                        if (result == null || result.terminalProof().isEmpty()) {
                            return CompletableFuture.completedFuture(new Result<>(
                                    operation, true, Optional.ofNullable(result).map(Completion::value), targets));
                        }
                        return release(attempted, result.terminalProof().orElseThrow())
                                .thenApply(left -> new Result<>(operation, true, Optional.of(result.value()), left));
                    });
        });
        // Cancelling an observer must not stop ticket acquisition, native termination, or reconciliation.
        return work.thenApply(result -> result);
    }

    /**
     * A concrete owner calls this only after its irreversible native selected/cancelled decision proves all old
     * attempts harmless. Reconciliation never invokes a writer and performs at most 256 ticket removals per pass.
     */
    public CompletionStage<List<PhysicalResourceIdV2>> reconcileTerminal(
            List<? extends PhysicalResourceIdV2> resources, Context context, Sha256Digest exactNativeTerminalProof) {
        var targets = canonicalTargets(resources);
        Objects.requireNonNull(context, "context");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(exactNativeTerminalProof, "exactNativeTerminalProof");
        var pending = new ArrayList<Attempt>();
        var unscanned = new TreeSet<PhysicalResourceIdV2>();
        CompletionStage<Void> scan = CompletableFuture.completedFuture(null);
        for (var resource : targets) {
            scan = scan.thenCompose(ignored -> {
                if (pending.size() == MAX_RECOVERY_TICKETS) {
                    unscanned.add(resource);
                    return CompletableFuture.completedFuture(null);
                }
                return coordinator.inspect(resource.authorityKey()).handle((value, failure) -> {
                    if (failure != null
                            || value.isEmpty()
                            || !value.orElseThrow().resource().equals(resource)) {
                        unscanned.add(resource);
                        return null;
                    }
                    var current = value.orElseThrow();
                    if (current.state() == TargetDeleteAuthorityStateV1.OPEN_V1) {
                        for (var ticket : current.fullAuthority().orElseThrow().activeWriterTickets()) {
                            if (context.matches(ticket)) {
                                if (pending.size() < MAX_RECOVERY_TICKETS) {
                                    pending.add(new Attempt(resource, ticket));
                                } else {
                                    unscanned.add(resource);
                                }
                            }
                        }
                    }
                    return null;
                });
            });
        }
        return scan.thenCompose(ignored -> release(pending, exactNativeTerminalProof))
                .thenApply(left -> {
                    var result = new TreeSet<PhysicalResourceIdV2>(unscanned);
                    result.addAll(left);
                    return List.copyOf(result);
                });
    }

    public static List<PhysicalResourceIdV2> canonicalTargets(List<? extends PhysicalResourceIdV2> resources) {
        Objects.requireNonNull(resources, "resources");
        if (resources.isEmpty() || resources.size() > MAX_TARGETS) {
            throw new IllegalArgumentException("physical writer target count is outside its bound");
        }
        int bytes = 0;
        var sorted = new TreeSet<PhysicalResourceIdV2>();
        for (var resource : resources) {
            Objects.requireNonNull(resource, "resource");
            bytes = Math.addExact(bytes, resource.canonicalBytes().length());
            if (bytes > MAX_TARGET_BYTES) {
                throw new IllegalArgumentException("physical writer target bytes exceed the bound");
            }
            sorted.add(resource);
        }
        return List.copyOf(sorted);
    }

    private CompletionStage<Boolean> acquire(
            PhysicalResourceIdV2 resource, Context context, Sha256Digest operation, List<Attempt> attempted) {
        return coordinator
                .inspect(resource.authorityKey())
                .thenCompose(value -> {
                    if (value.isEmpty()
                            || !value.orElseThrow().resource().equals(resource)
                            || value.orElseThrow().state() != TargetDeleteAuthorityStateV1.OPEN_V1) {
                        return CompletableFuture.completedFuture(false);
                    }
                    var full = value.orElseThrow().fullAuthority().orElseThrow();
                    if (full.activeWriterTickets().size() == M5TargetDeleteAuthorityRecordsV1.MAX_WRITER_TICKETS) {
                        return CompletableFuture.completedFuture(false);
                    }
                    var ticket = new ProofBoundWriterTicketV1(
                            context.writerClass(),
                            operation,
                            context.capabilitySha256(),
                            context.ownerFenceSha256(),
                            context.externalFactsRootSha256(),
                            full.authorityRevision());
                    var attempt = new Attempt(resource, ticket);
                    attempted.add(attempt);
                    return coordinator
                            .acquireWriterTicket(value.orElseThrow().exactStoredValue(), ticket)
                            .thenApply(result -> result.observed()
                                    .map(stored -> contains(stored, attempt))
                                    .orElse(false));
                })
                .exceptionally(failure -> false);
    }

    private CompletionStage<List<PhysicalResourceIdV2>> release(List<Attempt> attempted, Sha256Digest proof) {
        var unresolved = new TreeSet<PhysicalResourceIdV2>();
        CompletionStage<Void> chain = CompletableFuture.completedFuture(null);
        for (var attempt : attempted) {
            chain = chain.thenCompose(ignored -> releaseOne(attempt, proof).handle((done, failure) -> {
                if (failure != null || !done) {
                    unresolved.add(attempt.resource());
                }
                return null;
            }));
        }
        return chain.thenApply(ignored -> List.copyOf(unresolved));
    }

    private CompletionStage<Boolean> releaseOne(Attempt attempt, Sha256Digest proof) {
        return coordinator.inspect(attempt.resource().authorityKey()).thenCompose(value -> {
            if (value.isEmpty() || !value.orElseThrow().resource().equals(attempt.resource())) {
                return CompletableFuture.completedFuture(false);
            }
            var current = value.orElseThrow();
            if (!contains(current.exactStoredValue(), attempt)) {
                // A later revision excludes delayed acquisition CAS. An unchanged OPEN could still acquire late.
                return CompletableFuture.completedFuture(current.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                        || current.fullAuthority().orElseThrow().authorityRevision()
                                > attempt.ticket().predecessorAuthorityRevision());
            }
            return coordinator
                    .completeWriterTicket(
                            current.exactStoredValue(), attempt.ticket().operationIdSha256(), proof)
                    .thenApply(result -> result.observed()
                            .map(stored -> {
                                var observed = M5TargetDeleteStoredValueV2.decode(stored);
                                return observed.resource().equals(attempt.resource())
                                        && !contains(stored, attempt)
                                        && (observed.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                                                || observed.fullAuthority()
                                                                .orElseThrow()
                                                                .authorityRevision()
                                                        > attempt.ticket().predecessorAuthorityRevision());
                            })
                            .orElse(false));
        });
    }

    private static boolean contains(
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue stored,
            Attempt attempt) {
        var current = M5TargetDeleteStoredValueV2.decode(stored);
        return current.resource().equals(attempt.resource())
                && current.state() == TargetDeleteAuthorityStateV1.OPEN_V1
                && current.fullAuthority().orElseThrow().activeWriterTickets().contains(attempt.ticket());
    }
}
