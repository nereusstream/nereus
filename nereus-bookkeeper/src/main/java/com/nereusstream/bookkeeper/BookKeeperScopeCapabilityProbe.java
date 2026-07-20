/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperKeyspace;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.random.RandomGenerator;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;

/**
 * Real BookKeeper provider-scope canary with a durable QUARANTINED root reservation.
 *
 * <p>The permanent audit root is created before CreateAdv, so an ordinary allocator can never race the canary for the
 * same advanced ledger id. Only an exact NBKL1 match may be fenced or deleted after an uncertain provider response.
 */
public final class BookKeeperScopeCapabilityProbe {
    private static final int MAX_CANDIDATE_ATTEMPTS = 16;
    private static final int MAX_DELETE_ATTEMPTS = 3;
    private static final String DOMAIN = "NBKSCOPE1";
    private static final StreamId CANARY_STREAM = new StreamId("__nereus_bookkeeper_scope_canary_v1__");

    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerIdNamespaceReservation expectedNamespace;
    private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
    private final BookKeeperLedgerMetadataStore metadata;
    private final BookKeeperClientOperations client;
    private final BookKeeperPasswordProvider passwords;
    private final Clock clock;
    private final RandomGenerator random;
    private final BookKeeperKeyspace keys;

    public BookKeeperScopeCapabilityProbe(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation expectedNamespace,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperLedgerMetadataStore metadata,
            BookKeeperClientOperations client,
            BookKeeperPasswordProvider passwords,
            Clock clock) {
        this(
                cluster,
                configuration,
                expectedNamespace,
                namespaceVerifier,
                metadata,
                client,
                passwords,
                clock,
                new java.security.SecureRandom());
    }

    BookKeeperScopeCapabilityProbe(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation expectedNamespace,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperLedgerMetadataStore metadata,
            BookKeeperClientOperations client,
            BookKeeperPasswordProvider passwords,
            Clock clock,
            RandomGenerator random) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.expectedNamespace = Objects.requireNonNull(expectedNamespace, "expectedNamespace");
        this.namespaceVerifier = Objects.requireNonNull(namespaceVerifier, "namespaceVerifier");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.client = Objects.requireNonNull(client, "client");
        this.passwords = Objects.requireNonNull(passwords, "passwords");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        this.keys = new BookKeeperMetadataStoreConfig(
                        configuration.maxAppendRangesPerLedger(),
                        configuration.protectionSlotsPerRange(),
                        configuration.maxReaderLeasesPerLedger(),
                        configuration.maxUncertainAllocations())
                .keyspace(cluster);
        requireNamespace(expectedNamespace);
    }

    public CompletableFuture<BookKeeperScopeCapabilityProof> probe(
            BookKeeperScopeCapabilityRequest request) {
        final BookKeeperScopeCapabilityRequest exact;
        final BookKeeperOperationDeadline deadline;
        try {
            exact = Objects.requireNonNull(request, "request");
            deadline = new BookKeeperOperationDeadline(exact.timeout());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return namespaceVerifier.requireActive(configuration, deadline.remaining())
                .thenApply(this::requireNamespace)
                .thenCompose(namespace -> candidate(exact, namespace, deadline, 0))
                .thenCompose(proof -> namespaceVerifier
                        .requireActive(configuration, deadline.remaining())
                        .thenApply(this::requireNamespace)
                        .thenApply(ignored -> proof));
    }

    private CompletableFuture<BookKeeperScopeCapabilityProof> candidate(
            BookKeeperScopeCapabilityRequest request,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperOperationDeadline deadline,
            int attempt) {
        if (attempt >= MAX_CANDIDATE_ATTEMPTS) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_LIMIT_EXCEEDED,
                    false,
                    "BookKeeper scope canary exhausted its bounded advanced-ledger-id attempts"));
        }
        long ledgerId = configuration.ledgerIdNamespace().candidate(random);
        String allocationId = "scope-canary-" + BookKeeperIdentityDigests.sha256(
                request.runId() + ":" + attempt + ":" + ledgerId);
        BookKeeperLedgerCustomMetadata custom = BookKeeperLedgerCustomMetadata.create(
                cluster, configuration, namespace, CANARY_STREAM, attempt, allocationId);
        BookKeeperLedgerRootRecord root = quarantineRoot(
                request, namespace, ledgerId, attempt, allocationId, custom);
        return deadline.bound(metadata.createRoot(cluster, root))
                .handle((reserved, failure) -> {
                    if (failure == null) {
                        return create(request, reserved, custom, namespace, deadline, attempt);
                    }
                    Throwable cause = unwrap(failure);
                    if (cause instanceof BookKeeperMetadataConditionFailedException) {
                        return candidate(request, namespace, deadline, attempt + 1);
                    }
                    return CompletableFuture.<BookKeeperScopeCapabilityProof>failedFuture(cause);
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<BookKeeperScopeCapabilityProof> create(
            BookKeeperScopeCapabilityRequest request,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperLedgerCustomMetadata custom,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperOperationDeadline deadline,
            int attempt) {
        final byte[] password;
        try {
            password = Objects.requireNonNull(
                    passwords.resolve(configuration.passwordRef()), "resolved password");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<WriteAdvHandle> create;
        try {
            create = client.createAdvanced(
                    root.value().ledgerId(),
                    configuration,
                    password,
                    custom.values(),
                    providerDeadline(deadline));
        } catch (Throwable failure) {
            create = CompletableFuture.failedFuture(failure);
        }
        return create.handle((handle, failure) -> {
                    if (failure == null) {
                        return runCanary(request, root, custom, handle, deadline);
                    }
                    return recoverCreateFailure(
                            request,
                            root,
                            custom,
                            namespace,
                            deadline,
                            attempt,
                            unwrap(failure));
                })
                .thenCompose(Function.identity())
                .whenComplete((ignored, failure) -> Arrays.fill(password, (byte) 0));
    }

    private CompletableFuture<BookKeeperScopeCapabilityProof> recoverCreateFailure(
            BookKeeperScopeCapabilityRequest request,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperLedgerCustomMetadata custom,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperOperationDeadline deadline,
            int attempt,
            Throwable createFailure) {
        long ledgerId = root.value().ledgerId();
        return client.metadata(ledgerId, providerDeadline(deadline))
                .handle((providerMetadata, metadataFailure) -> {
                    if (metadataFailure != null) {
                        return CompletableFuture.<BookKeeperScopeCapabilityProof>failedFuture(
                                new NereusException(
                                        ErrorCode.PRIMARY_WAL_WRITE_FAILED,
                                        false,
                                        "BookKeeper scope canary create outcome remains unknown; its durable root is quarantined",
                                        createFailure));
                    }
                    try {
                        custom.requireExactImmutableLedgerMetadata(
                                ledgerId, configuration, providerMetadata);
                    } catch (Throwable foreign) {
                        return candidate(request, namespace, deadline, attempt + 1);
                    }
                    return fenceDeleteAndRetry(
                            request, root, namespace, deadline, attempt, createFailure);
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<BookKeeperScopeCapabilityProof> fenceDeleteAndRetry(
            BookKeeperScopeCapabilityRequest request,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperOperationDeadline deadline,
            int attempt,
            Throwable createFailure) {
        byte[] password = password();
        return client.open(
                        root.value().ledgerId(),
                        configuration.digestType(),
                        password,
                        true,
                        providerDeadline(deadline))
                .whenComplete((ignored, failure) -> Arrays.fill(password, (byte) 0))
                .thenCompose(handle -> close(handle, deadline))
                .thenCompose(ignored -> deleteAndConfirm(root.value().ledgerId(), deadline, 0))
                .thenCompose(ignored -> candidate(request, namespace, deadline, attempt + 1))
                .exceptionallyCompose(cleanupFailure -> CompletableFuture.failedFuture(
                        new NereusException(
                                ErrorCode.PRIMARY_WAL_WRITE_FAILED,
                                false,
                                "BookKeeper scope canary could not clean an exact uncertain create",
                                combine(createFailure, unwrap(cleanupFailure)))));
    }

    private CompletableFuture<BookKeeperScopeCapabilityProof> runCanary(
            BookKeeperScopeCapabilityRequest request,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperLedgerCustomMetadata custom,
            WriteAdvHandle handle,
            BookKeeperOperationDeadline deadline) {
        final byte[] payload;
        try {
            requireHandle(root, custom, handle);
            payload = payload();
        } catch (Throwable failure) {
            return close(handle, deadline)
                    .thenCompose(ignored -> CompletableFuture.failedFuture(failure));
        }
        ByteBuf entry = Unpooled.wrappedBuffer(payload);
        CompletableFuture<BookKeeperScopeCapabilityProof> canary = client
                .write(handle, 0, entry, providerDeadline(deadline))
                .thenApply(written -> {
                    if (written != 0) {
                        throw invariant("BookKeeper scope canary wrote another entry id");
                    }
                    return null;
                })
                .thenCompose(ignored -> closeBestEffort(handle, deadline))
                .thenCompose(ignored -> readExact(root, custom, payload, false, deadline))
                .thenCompose(ignored -> readExact(root, custom, payload, true, deadline))
                .thenCompose(ignored -> deleteAndConfirm(root.value().ledgerId(), deadline, 0))
                .thenApply(ignored -> proof(request, root.value().ledgerId(), payload));
        return canary.handle((proof, failure) -> {
                    entry.release();
                    if (failure == null) {
                        return CompletableFuture.completedFuture(proof);
                    }
                    Throwable cause = unwrap(failure);
                    return cleanupExact(root.value().ledgerId(), handle, deadline)
                            .handle((ignored, cleanupFailure) -> {
                                if (cleanupFailure != null) {
                                    cause.addSuppressed(unwrap(cleanupFailure));
                                }
                                return CompletableFuture.<BookKeeperScopeCapabilityProof>failedFuture(cause);
                            })
                            .thenCompose(Function.identity());
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Void> readExact(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperLedgerCustomMetadata custom,
            byte[] expected,
            boolean recovery,
            BookKeeperOperationDeadline deadline) {
        byte[] password = password();
        return client.open(
                        root.value().ledgerId(),
                        configuration.digestType(),
                        password,
                        recovery,
                        providerDeadline(deadline))
                .whenComplete((ignored, failure) -> Arrays.fill(password, (byte) 0))
                .thenCompose(handle -> {
                    try {
                        custom.requireExactImmutableLedgerMetadata(
                                root.value().ledgerId(),
                                configuration,
                                handle.getLedgerMetadata());
                        if (recovery && !handle.getLedgerMetadata().isClosed()) {
                            throw invariant("BookKeeper scope canary recovery open did not fence/close the ledger");
                        }
                    } catch (Throwable failure) {
                        return close(handle, deadline)
                                .thenCompose(ignored -> CompletableFuture.failedFuture(failure));
                    }
                    return client.readUnconfirmed(
                                    handle, 0, 0, providerDeadline(deadline))
                            .thenAccept(entries -> requirePayload(entries, expected))
                            .handle((ignored, readFailure) -> close(handle, deadline)
                                    .handle((closed, closeFailure) -> {
                                        if (readFailure != null) {
                                            Throwable cause = unwrap(readFailure);
                                            if (closeFailure != null) {
                                                cause.addSuppressed(unwrap(closeFailure));
                                            }
                                            throw new CompletionException(cause);
                                        }
                                        if (closeFailure != null) {
                                            throw new CompletionException(unwrap(closeFailure));
                                        }
                                        return (Void) null;
                                    }))
                            .thenCompose(Function.identity());
                });
    }

    private CompletableFuture<Void> deleteAndConfirm(
            long ledgerId, BookKeeperOperationDeadline deadline, int attempt) {
        if (attempt >= MAX_DELETE_ATTEMPTS) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.PRIMARY_WAL_WRITE_FAILED,
                    false,
                    "BookKeeper scope canary remained present after bounded delete attempts"));
        }
        CompletableFuture<Void> delete;
        try {
            delete = client.delete(ledgerId, providerDeadline(deadline));
        } catch (Throwable failure) {
            delete = CompletableFuture.failedFuture(failure);
        }
        return delete.handle((ignored, failure) -> null)
                .thenCompose(ignored -> absent(ledgerId, deadline))
                .thenCompose(absent -> absent
                        ? absent(ledgerId, deadline).thenCompose(second -> {
                            if (!second) {
                                return CompletableFuture.failedFuture(invariant(
                                        "BookKeeper scope canary reappeared after deletion"));
                            }
                            return CompletableFuture.completedFuture(null);
                        })
                        : deleteAndConfirm(ledgerId, deadline, attempt + 1));
    }

    private CompletableFuture<Boolean> absent(
            long ledgerId, BookKeeperOperationDeadline deadline) {
        return client.metadata(ledgerId, providerDeadline(deadline))
                .handle((value, failure) -> {
                    if (failure == null) {
                        return false;
                    }
                    Throwable cause = unwrap(failure);
                    if (cause instanceof NereusException nereus
                            && nereus.code() == ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND) {
                        return true;
                    }
                    throw new CompletionException(cause);
                });
    }

    private CompletableFuture<Void> cleanupExact(
            long ledgerId,
            WriteAdvHandle handle,
            BookKeeperOperationDeadline deadline) {
        return closeBestEffort(handle, deadline)
                .thenCompose(ignored -> deleteAndConfirm(ledgerId, deadline, 0));
    }

    private CompletableFuture<Void> closeBestEffort(
            WriteAdvHandle handle, BookKeeperOperationDeadline deadline) {
        return close(handle, deadline).handle((ignored, failure) -> null);
    }

    private CompletableFuture<Void> close(
            org.apache.bookkeeper.client.api.Handle handle,
            BookKeeperOperationDeadline deadline) {
        try {
            return deadline.bound(handle.closeAsync());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private void requireHandle(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperLedgerCustomMetadata custom,
            WriteAdvHandle handle) {
        if (handle.getId() != root.value().ledgerId()) {
            throw invariant("BookKeeper scope canary CreateAdv returned another ledger id");
        }
        custom.requireExactImmutableLedgerMetadata(
                root.value().ledgerId(), configuration, handle.getLedgerMetadata());
    }

    private static void requirePayload(LedgerEntries entries, byte[] expected) {
        try (LedgerEntries exact = entries) {
            java.util.Iterator<LedgerEntry> iterator = exact.iterator();
            if (!iterator.hasNext()) {
                throw invariant("BookKeeper scope canary read returned no entry");
            }
            LedgerEntry entry = iterator.next();
            if (entry.getLength() < 0 || entry.getLength() > Integer.MAX_VALUE) {
                throw invariant("BookKeeper scope canary read returned an invalid entry length");
            }
            byte[] actual = new byte[Math.toIntExact(entry.getLength())];
            entry.getEntryBuffer().getBytes(entry.getEntryBuffer().readerIndex(), actual);
            if (entry.getEntryId() != 0 || iterator.hasNext() || !Arrays.equals(expected, actual)) {
                throw invariant("BookKeeper scope canary read returned different bytes or identity");
            }
        }
    }

    private BookKeeperLedgerRootRecord quarantineRoot(
            BookKeeperScopeCapabilityRequest request,
            BookKeeperLedgerIdNamespaceReservation namespace,
            long ledgerId,
            int attempt,
            String allocationId,
            BookKeeperLedgerCustomMetadata custom) {
        long now = Math.max(0, clock.millis());
        String ledgerIdentity = keys.ledgerIdentitySha256(
                configuration.providerScopeSha256(), ledgerId);
        String runHash = BookKeeperIdentityDigests.sha256(request.runId());
        return new BookKeeperLedgerRootRecord(
                1,
                ledgerIdentity,
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                ledgerId,
                CANARY_STREAM.value(),
                attempt,
                allocationId,
                0,
                configuration.configurationBindingSha256().value(),
                namespace.ledgerIdNamespaceSha256().value(),
                false,
                "nereus-bookkeeper-scope-canary",
                runHash,
                0,
                BookKeeperIdentityDigests.sha256("scope-canary-fence/" + request.runId()),
                configuration.ensembleSize(),
                configuration.writeQuorumSize(),
                configuration.ackQuorumSize(),
                configuration.digestType().name(),
                custom.sha256().value(),
                BookKeeperLedgerLifecycle.QUARANTINED,
                1,
                now,
                0,
                0,
                0,
                -1,
                0,
                "",
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "administrative provider-scope canary reservation",
                0);
    }

    private BookKeeperScopeCapabilityProof proof(
            BookKeeperScopeCapabilityRequest request,
            long ledgerId,
            byte[] payload) {
        Checksum payloadSha = new Checksum(
                ChecksumType.SHA256,
                HexFormat.of().formatHex(sha256().digest(payload)));
        MessageDigest digest = sha256();
        frame(digest, DOMAIN);
        frame(digest, cluster);
        frame(digest, configuration.configurationBindingSha256().value());
        frame(digest, expectedNamespace.ledgerIdNamespaceSha256().value());
        frame(digest, request.runId());
        number(digest, request.readiness().brokerReadinessEpoch());
        frame(digest, request.readiness().brokerSetSha256().value());
        number(digest, ledgerId);
        frame(digest, payloadSha.value());
        frame(digest, "create-write-read-fence-delete-dual-absence");
        return new BookKeeperScopeCapabilityProof(
                request.runId(),
                request.readiness().brokerReadinessEpoch(),
                request.readiness().brokerSetSha256(),
                ledgerId,
                payloadSha,
                new Checksum(
                        ChecksumType.SHA256,
                        HexFormat.of().formatHex(digest.digest())));
    }

    private byte[] payload() {
        MessageDigest digest = sha256();
        frame(digest, "nereus-bookkeeper-primary-wal-scope-canary-payload-v1");
        frame(digest, cluster);
        frame(digest, configuration.configurationBindingSha256().value());
        frame(digest, expectedNamespace.ledgerIdNamespaceSha256().value());
        return digest.digest();
    }

    private byte[] password() {
        return Objects.requireNonNull(
                passwords.resolve(configuration.passwordRef()), "resolved password");
    }

    private BookKeeperLedgerIdNamespaceReservation requireNamespace(
            BookKeeperLedgerIdNamespaceReservation actual) {
        BookKeeperLedgerIdNamespaceReservation exact = Objects.requireNonNull(actual, "namespace");
        if (!exact.equals(expectedNamespace)) {
            throw new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    "BookKeeper namespace changed around the provider-scope canary");
        }
        return exact;
    }

    private BookKeeperOperationDeadline providerDeadline(
            BookKeeperOperationDeadline overall) {
        return new BookKeeperOperationDeadline(min(
                overall.remaining(), configuration.operationTimeout()));
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static Throwable combine(Throwable primary, Throwable secondary) {
        Throwable exact = unwrap(primary);
        Throwable suppressed = unwrap(secondary);
        if (exact != suppressed) {
            exact.addSuppressed(suppressed);
        }
        return exact;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static void frame(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static void number(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static String text(String value, String name) {
        String exact = Objects.requireNonNull(value, name);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return exact;
    }
}
