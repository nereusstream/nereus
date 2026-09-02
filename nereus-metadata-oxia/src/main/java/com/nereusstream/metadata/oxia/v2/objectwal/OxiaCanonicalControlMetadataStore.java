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

package com.nereusstream.metadata.oxia.v2.objectwal;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

/**
 * Synchronous exact-byte M3 control adapter over one source-locked O1 client.
 *
 * <p>The public key is the accepted relative Object-WAL key. The backend key is always rooted below
 * one immutable Cell authority root, and the adapter accepts only the M3 run-control and M4
 * Binding read-control families for its configured shard. It does not expose a general Oxia
 * key/value capability.
 */
public final class OxiaCanonicalControlMetadataStore implements CanonicalControlMetadataStore {
    private static final int MAX_RELATIVE_KEY_BYTES = 1_024;
    private static final int MAX_AUTHORITY_ROOT_BYTES = 512;
    private static final int MAX_OXIA_KEY_BYTES = 512;
    private static final Pattern AUTHORITY_ROOT = Pattern.compile("/(?:[A-Za-z0-9_-]+)(?:/[A-Za-z0-9_-]+)*");
    private static final String RUN = "/runs/[0-9]{20}";
    private static final String PAGE = "/checkpoint/pages/[0-9]{20}-[0-9a-f]{64}";
    private static final String KAFKA_HEAD = "/protocol/kafka/nwkcp1-v1/head";
    private static final String M4_BINDING = "/read-m4/[0-9a-f]{64}";
    private static final String M4_ORDINAL = "/[0-9]{20}";
    private static final String M4_PROTECTION = "/protections/[0-9a-f]{64}-[0-9]{20}";

    private final OxiaConditionalClient client;
    private final MutationFailureClassifier failureClassifier;
    private final String cellAuthorityRoot;
    private final Pattern acceptedRelativeKey;

    public OxiaCanonicalControlMetadataStore(AsyncOxiaClient client, String cellAuthorityRoot, int shardId) {
        this(new AsyncOxiaConditionalClient(client), cellAuthorityRoot, shardId);
    }

    /** Narrow-client constructor used by deterministic conformance tests and controlled adapters. */
    public OxiaCanonicalControlMetadataStore(OxiaConditionalClient client, String cellAuthorityRoot, int shardId) {
        this.client = Objects.requireNonNull(client, "client");
        this.failureClassifier = new MutationFailureClassifier();
        this.cellAuthorityRoot = requireAuthorityRoot(cellAuthorityRoot);
        if (shardId < 0) {
            throw new IllegalArgumentException("shard ID must be non-negative");
        }
        String shard = String.format(java.util.Locale.ROOT, "%010d", shardId);
        String prefix = "v2/object-wal/shards/" + shard;
        String longestRunKey =
                prefix + "/runs/" + "0".repeat(20) + "/checkpoint/pages/" + "0".repeat(20) + "-" + "0".repeat(64);
        String longestM4Key =
                prefix + "/read-m4/" + "0".repeat(64) + "/protections/" + "0".repeat(64) + "-" + "0".repeat(20);
        String longestRelativeKey = longestRunKey.length() >= longestM4Key.length() ? longestRunKey : longestM4Key;
        if (this.cellAuthorityRoot.length() + 1 + longestRelativeKey.length() > MAX_OXIA_KEY_BYTES) {
            throw new IllegalArgumentException("Cell authority root leaves insufficient room for bounded Oxia keys");
        }
        acceptedRelativeKey = Pattern.compile(Pattern.quote(prefix)
                + "(?:/current|"
                + RUN
                + "(?:/root|/seal|/checkpoint/head|"
                + PAGE
                + "|"
                + KAFKA_HEAD
                + ")|"
                + M4_BINDING
                + "(?:/selector|/proof-head|/capabilities"
                + M4_ORDINAL
                + "|/terminals"
                + M4_ORDINAL
                + "|/proofs"
                + M4_ORDINAL
                + "|"
                + M4_PROTECTION
                + "))");
    }

    @Override
    public Optional<CanonicalBytes> get(String key) {
        return readRequired(backendKey(key)).map(AuthorityRecord::storedBytes);
    }

    @Override
    public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes exactValue) {
        String backendKey = backendKey(key);
        CanonicalBytes candidate = requireValue(exactValue);
        MutationAttempt attempt = dispatch(() -> client.createIfAbsent(backendKey, candidate));
        return reconcile(backendKey, candidate, attempt);
    }

    @Override
    public ControlMutationOutcome compareAndSet(
            String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes exactCandidate) {
        String backendKey = backendKey(key);
        Optional<CanonicalBytes> expected = Objects.requireNonNull(exactExpected, "exactExpected")
                .map(OxiaCanonicalControlMetadataStore::requireValue);
        CanonicalBytes candidate = requireValue(exactCandidate);

        if (expected.isEmpty()) {
            MutationAttempt attempt = dispatch(() -> client.createIfAbsent(backendKey, candidate));
            return reconcile(backendKey, candidate, attempt);
        }

        Optional<AuthorityRecord> before;
        try {
            before = readRequired(backendKey);
        } catch (ControlMetadataBackendException failure) {
            return ControlMutationOutcome.RESPONSE_UNKNOWN;
        }
        if (before.isEmpty() || !before.orElseThrow().storedBytes().equals(expected.orElseThrow())) {
            return ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        long expectedVersionId = before.orElseThrow().versionId();
        MutationAttempt attempt = dispatch(() -> client.compareAndSet(backendKey, candidate, expectedVersionId));
        return reconcile(backendKey, candidate, attempt);
    }

    private ControlMutationOutcome reconcile(String backendKey, CanonicalBytes candidate, MutationAttempt attempt) {
        Optional<AuthorityRecord> after;
        try {
            after = readRequired(backendKey);
        } catch (ControlMetadataBackendException failure) {
            return ControlMutationOutcome.RESPONSE_UNKNOWN;
        }
        if (after.isPresent() && after.orElseThrow().storedBytes().equals(candidate)) {
            return ControlMutationOutcome.APPLIED;
        }
        if (attempt.succeeded() || attempt.failureKind() == MutationFailureClassifier.Kind.RESPONSE_UNKNOWN) {
            return ControlMutationOutcome.RESPONSE_UNKNOWN;
        }
        return ControlMutationOutcome.DEFINITIVE_CONFLICT;
    }

    private MutationAttempt dispatch(MutationDispatch mutation) {
        try {
            await(Objects.requireNonNull(mutation.run(), "conditional mutation stage"));
            return MutationAttempt.success();
        } catch (RuntimeException failure) {
            return MutationAttempt.failed(failureClassifier.classify(failure));
        }
    }

    private Optional<AuthorityRecord> readRequired(String backendKey) {
        try {
            Optional<AuthorityRecord> record = await(Objects.requireNonNull(client.read(backendKey), "read stage"));
            Objects.requireNonNull(record, "read result");
            record.ifPresent(value -> {
                if (!backendKey.equals(value.key())) {
                    throw new ControlMetadataBackendException("Oxia returned a different control-metadata key");
                }
            });
            return record;
        } catch (ControlMetadataBackendException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new ControlMetadataBackendException("Oxia exact-key control-metadata read failed", failure);
        }
    }

    private String backendKey(String relativeKey) {
        Objects.requireNonNull(relativeKey, "key");
        if (relativeKey.length() > MAX_RELATIVE_KEY_BYTES
                || !acceptedRelativeKey.matcher(relativeKey).matches()) {
            throw new IllegalArgumentException(
                    "control-metadata key is outside the exact configured Cell/shard grammar: " + relativeKey);
        }
        String backendKey = cellAuthorityRoot + "/" + relativeKey;
        if (backendKey.length() > MAX_OXIA_KEY_BYTES) {
            throw new IllegalArgumentException("control-metadata Oxia key exceeds 512 ASCII bytes");
        }
        return backendKey;
    }

    private static String requireAuthorityRoot(String root) {
        Objects.requireNonNull(root, "cellAuthorityRoot");
        if (root.length() > MAX_AUTHORITY_ROOT_BYTES
                || !AUTHORITY_ROOT.matcher(root).matches()) {
            throw new IllegalArgumentException("Cell authority root must be one canonical absolute Oxia key prefix");
        }
        return root;
    }

    private static CanonicalBytes requireValue(CanonicalBytes value) {
        return Objects.requireNonNull(value, "exact control-metadata value");
    }

    private static <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().join();
        } catch (CompletionException failure) {
            throw failure;
        }
    }

    @FunctionalInterface
    private interface MutationDispatch {
        CompletionStage<Void> run();
    }

    private record MutationAttempt(boolean succeeded, MutationFailureClassifier.Kind failureKind) {
        private static MutationAttempt success() {
            return new MutationAttempt(true, null);
        }

        private static MutationAttempt failed(MutationFailureClassifier.Kind kind) {
            return new MutationAttempt(false, Objects.requireNonNull(kind, "kind"));
        }
    }

    /** A backend read failed and therefore cannot be represented as authoritative absence. */
    public static final class ControlMetadataBackendException extends IllegalStateException {
        private ControlMetadataBackendException(String message) {
            super(message);
        }

        private ControlMetadataBackendException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
