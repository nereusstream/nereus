/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Uses one isolated canary to prove guarded PUT, exact HEAD, complete LIST, exact DELETE,
 * delete-response-loss convergence and post-delete absence for the configured object-store scope.
 */
public final class DefaultObjectStoreDeleteCapabilityProbe
        implements ObjectStoreDeleteCapabilityProbe {
    private static final String PROTOCOL = "nereus-object-store-delete-capability-v1";
    private static final String PROBE_PREFIX = "__nereus_capability__/delete-v1/";

    private final ObjectStore objectStore;
    private final ObjectStoreConfiguration configuration;
    private final Clock clock;
    private final LongSupplier nanoTime;
    private final String capabilitySha256;

    public DefaultObjectStoreDeleteCapabilityProbe(
            ObjectStore objectStore,
            ObjectStoreConfiguration configuration,
            Clock clock) {
        this(objectStore, configuration, clock, System::nanoTime);
    }

    DefaultObjectStoreDeleteCapabilityProbe(
            ObjectStore objectStore,
            ObjectStoreConfiguration configuration,
            Clock clock,
            LongSupplier nanoTime) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.capabilitySha256 = capabilityIdentity(configuration);
    }

    @Override
    public String expectedCapabilitySha256() {
        return capabilitySha256;
    }

    @Override
    public CompletableFuture<ObjectStoreDeleteCapabilityProof> probe(
            ObjectStoreDeleteCapabilityRequest request) {
        ObjectStoreDeleteCapabilityRequest exact = Objects.requireNonNull(request, "request");
        ProbeContext context = context(exact);
        Deadline deadline;
        try {
            deadline = Deadline.start(exact.timeout(), nanoTime);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<ObjectStoreDeleteCapabilityProof> attempt = putOrVerify(context, deadline)
                .thenCompose(head -> verifyPresentList(context, head, deadline)
                        .thenApply(ignored -> head))
                .thenCompose(head -> deleteAndVerify(context, head, deadline))
                .thenCompose(ignored -> verifyIdempotentDelete(context, deadline))
                .thenCompose(ignored -> verifyAbsentList(context, deadline))
                .thenApply(ignored -> new ObjectStoreDeleteCapabilityProof(
                        ObjectStoreDeleteCapabilityProof.PROTOCOL_VERSION,
                        capabilitySha256,
                        sha256(context.key().value().getBytes(StandardCharsets.UTF_8)),
                        Math.max(1, clock.millis())));
        return attempt.handle((proof, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(proof);
                    }
                    Throwable cause = unwrap(failure);
                    return cleanup(context, exact.timeout()).handle((ignored, cleanupFailure) -> {
                                if (cleanupFailure != null) {
                                    cause.addSuppressed(unwrap(cleanupFailure));
                                }
                                return CompletableFuture
                                        .<ObjectStoreDeleteCapabilityProof>failedFuture(cause);
                            })
                            .thenCompose(value -> value);
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<HeadObjectResult> putOrVerify(
            ProbeContext context, Deadline deadline) {
        CompletableFuture<PutObjectResult> put;
        try {
            put = deadline.call(() -> objectStore.putObject(
                    context.key(),
                    ByteBuffer.wrap(context.payload()).asReadOnlyBuffer(),
                    new PutObjectOptions(
                            "application/octet-stream",
                            context.checksum(),
                            true,
                            Map.of("nereus-capability-probe", "delete-v1"),
                            deadline.remaining())));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return put.handle((result, putFailure) -> putFailure)
                .thenCompose(putFailure -> head(context, deadline)
                        .handle((head, headFailure) -> {
                            if (headFailure == null) {
                                verifyHead(context, head);
                                return head;
                            }
                            Throwable cause = putFailure == null
                                    ? unwrap(headFailure)
                                    : unwrap(putFailure);
                            if (putFailure != null) {
                                cause.addSuppressed(unwrap(headFailure));
                            }
                            throw new CompletionException(cause);
                        }));
    }

    private CompletableFuture<Void> verifyPresentList(
            ProbeContext context,
            HeadObjectResult head,
            Deadline deadline) {
        return list(context, deadline).thenAccept(page -> {
            if (page.continuationToken().isPresent() || page.objects().size() != 1) {
                throw capabilityFailure("probe LIST did not return one complete exact page");
            }
            ListedObject listed = page.objects().get(0);
            if (!listed.key().equals(context.key())
                    || listed.objectLength() != context.payload().length
                    || listed.lastModified().isEmpty()
                    || listed.etag().filter(value -> head.etag().filter(value::equals).isEmpty())
                            .isPresent()) {
                throw capabilityFailure("probe LIST identity does not match exact HEAD");
            }
        });
    }

    private CompletableFuture<Void> deleteAndVerify(
            ProbeContext context,
            HeadObjectResult head,
            Deadline deadline) {
        DeleteObjectOptions options = deleteOptions(context, head.etag(), deadline.remaining());
        CompletableFuture<DeleteObjectResult> deletion;
        try {
            deletion = deadline.call(() -> objectStore.deleteObject(context.key(), options));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return deletion.handle((result, failure) -> failure)
                .thenCompose(deleteFailure -> verifyAbsent(context, deadline)
                        .handle((ignored, absenceFailure) -> {
                            if (absenceFailure == null) {
                                return null;
                            }
                            Throwable cause = deleteFailure == null
                                    ? unwrap(absenceFailure)
                                    : unwrap(deleteFailure);
                            if (deleteFailure != null) {
                                cause.addSuppressed(unwrap(absenceFailure));
                            }
                            throw new CompletionException(cause);
                        }));
    }

    private CompletableFuture<Void> verifyIdempotentDelete(
            ProbeContext context, Deadline deadline) {
        DeleteObjectOptions options = deleteOptions(context, Optional.empty(), deadline.remaining());
        return deadline.call(() -> objectStore.deleteObject(context.key(), options))
                .thenAccept(result -> {
                    if (!result.key().equals(context.key())
                            || result.status() != DeleteObjectResult.Status.ALREADY_ABSENT) {
                        throw capabilityFailure("probe DELETE absence is not idempotent");
                    }
                });
    }

    private CompletableFuture<Void> verifyAbsentList(
            ProbeContext context, Deadline deadline) {
        return list(context, deadline).thenAccept(page -> {
            if (!page.objects().isEmpty() || page.continuationToken().isPresent()) {
                throw capabilityFailure("probe object remains visible after exact DELETE");
            }
        });
    }

    private CompletableFuture<HeadObjectResult> head(
            ProbeContext context, Deadline deadline) {
        return deadline.call(() -> objectStore.headObject(
                context.key(), new HeadObjectOptions(deadline.remaining())));
    }

    private CompletableFuture<ListObjectsResult> list(
            ProbeContext context, Deadline deadline) {
        return deadline.call(() -> objectStore.listObjects(
                context.prefix(),
                Optional.empty(),
                new ListObjectsOptions(2, deadline.remaining())));
    }

    private CompletableFuture<Void> verifyAbsent(
            ProbeContext context, Deadline deadline) {
        return head(context, deadline).handle((value, failure) -> {
            if (failure == null) {
                throw capabilityFailure("probe object still exists after exact DELETE");
            }
            Throwable cause = unwrap(failure);
            if (!isNotFound(cause)) {
                throw new CompletionException(cause);
            }
            return null;
        });
    }

    private CompletableFuture<Void> cleanup(ProbeContext context, Duration timeout) {
        Duration bounded = minimum(timeout, configuration.requestTimeout());
        HeadObjectOptions headOptions = new HeadObjectOptions(bounded);
        return objectStore.headObject(context.key(), headOptions)
                .handle((head, failure) -> {
                    if (failure != null) {
                        Throwable cause = unwrap(failure);
                        if (isNotFound(cause)) {
                            return CompletableFuture.<Void>completedFuture(null);
                        }
                        return CompletableFuture.<Void>failedFuture(cause);
                    }
                    try {
                        verifyHead(context, head);
                    } catch (Throwable mismatch) {
                        return CompletableFuture.<Void>failedFuture(mismatch);
                    }
                    return objectStore.deleteObject(
                                    context.key(),
                                    deleteOptions(context, head.etag(), bounded))
                            .<Void>thenApply(ignored -> null);
                })
                .thenCompose(value -> value);
    }

    private static void verifyHead(ProbeContext context, HeadObjectResult head) {
        if (!head.key().equals(context.key())
                || head.objectLength() != context.payload().length
                || !head.checksum().equals(context.checksum())
                || head.etag().isEmpty()) {
            throw capabilityFailure("probe HEAD does not match immutable canary identity");
        }
    }

    private static DeleteObjectOptions deleteOptions(
            ProbeContext context, Optional<String> etag, Duration timeout) {
        return new DeleteObjectOptions(
                context.payload().length,
                context.checksum(),
                etag,
                timeout);
    }

    private ProbeContext context(ObjectStoreDeleteCapabilityRequest request) {
        ObjectKeyPrefix prefix = new ObjectKeyPrefix(PROBE_PREFIX + request.runId() + "/");
        ObjectKey key = new ObjectKey(prefix.value() + "probe");
        byte[] payload = (PROTOCOL + "\n" + capabilitySha256 + "\n" + request.runId() + "\n")
                .getBytes(StandardCharsets.UTF_8);
        return new ProbeContext(key, prefix, payload, Crc32cChecksums.checksum(payload));
    }

    private static String capabilityIdentity(ObjectStoreConfiguration configuration) {
        CapabilityDigest digest = new CapabilityDigest();
        digest.text(PROTOCOL);
        digest.text(configuration.providerClassName());
        digest.text(configuration.endpoint().normalize().toASCIIString().toLowerCase(Locale.ROOT));
        digest.text(configuration.region());
        digest.text(configuration.bucket());
        digest.text(configuration.prefix());
        digest.text(Boolean.toString(configuration.pathStyleAccess()));
        digest.text("guarded-put-if-absent");
        digest.text("exact-head-crc32c-etag");
        digest.text("complete-prefix-list-with-last-modified");
        digest.text("exact-identity-delete");
        digest.text("delete-response-loss-absence-recovery");
        digest.text("idempotent-delete-and-post-delete-list-absence");
        return digest.finish();
    }

    private static Duration minimum(Duration first, Duration second) {
        return first.compareTo(second) <= 0 ? first : second;
    }

    private static boolean isNotFound(Throwable failure) {
        return failure instanceof NereusException nereus
                && nereus.code() == ErrorCode.OBJECT_NOT_FOUND;
    }

    private static NereusException capabilityFailure(String message) {
        return new NereusException(
                ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, message);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable failure = supplied;
        while ((failure instanceof CompletionException
                        || failure instanceof java.util.concurrent.ExecutionException)
                && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private record ProbeContext(
            ObjectKey key,
            ObjectKeyPrefix prefix,
            byte[] payload,
            Checksum checksum) {
        private ProbeContext {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(prefix, "prefix");
            payload = Objects.requireNonNull(payload, "payload").clone();
            Objects.requireNonNull(checksum, "checksum");
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }

    private static final class CapabilityDigest {
        private final MessageDigest digest;

        private CapabilityDigest() {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("SHA-256 is unavailable", impossible);
            }
        }

        private void text(String value) {
            byte[] bytes = Objects.requireNonNull(value, "value")
                    .getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
            digest.update(bytes);
        }

        private String finish() {
            return HexFormat.of().formatHex(digest.digest());
        }
    }

    private static final class Deadline {
        private final long deadlineNanos;
        private final LongSupplier nanoTime;

        private Deadline(long deadlineNanos, LongSupplier nanoTime) {
            this.deadlineNanos = deadlineNanos;
            this.nanoTime = nanoTime;
        }

        private static Deadline start(Duration timeout, LongSupplier nanoTime) {
            Objects.requireNonNull(timeout, "timeout");
            Objects.requireNonNull(nanoTime, "nanoTime");
            try {
                return new Deadline(
                        Math.addExact(nanoTime.getAsLong(), timeout.toNanos()), nanoTime);
            } catch (ArithmeticException failure) {
                throw new IllegalArgumentException("probe deadline overflows", failure);
            }
        }

        private Duration remaining() {
            long nanos = deadlineNanos - nanoTime.getAsLong();
            if (nanos <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT, true, "object-store capability probe timed out");
            }
            long millis = Math.max(1, (nanos + 999_999L) / 1_000_000L);
            return Duration.ofMillis(millis);
        }

        private <T> CompletableFuture<T> call(
                Supplier<CompletableFuture<T>> operation) {
            Duration bounded = remaining();
            final CompletableFuture<T> future;
            try {
                future = Objects.requireNonNull(operation.get(), "operation future");
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
            return future.orTimeout(bounded.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
