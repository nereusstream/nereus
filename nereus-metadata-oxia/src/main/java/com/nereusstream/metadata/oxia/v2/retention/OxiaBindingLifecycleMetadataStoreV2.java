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

package com.nereusstream.metadata.oxia.v2.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityControlMetadataStoreV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5BindingBootstrapV1;
import com.nereusstream.storage.object.retention.M5BindingLogicalTrimCoordinatorV2;
import com.nereusstream.storage.object.retention.M5LogicalTrimCoordinatorV1;
import com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2;
import com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2.LogicalTrimMutationV2;
import com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2.LogicalTrimStateV2;
import com.nereusstream.storage.object.retention.M5ReferenceFreshnessVerifierV1;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionKeysV1;
import com.nereusstream.storage.object.retention.M5RetentionPlannerV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BindingTrimFrontierV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import com.nereusstream.storage.object.retention.M5RetiredBatchHistoryV2;
import com.nereusstream.storage.object.retention.M5SerializedLogicalTrimStoreV2;
import com.nereusstream.storage.object.retention.M5TaskSelectionDecisionV2;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

/**
 * One admitted Cell root/shard/Binding route for exact M4 control and immutable M5 history.
 *
 * <p>Public keys remain canonical relative keys, including VersionedValue keys. Only this boundary adds the Cell
 * root. The returned synchronous control view belongs on the owner's low-frequency control executor; asynchronous
 * history operations never block a native callback.
 * This route does not grant owner, namespace-quota or writer authority.
 */
public final class OxiaBindingLifecycleMetadataStoreV2
        implements ExactMetadataTransactionStoreV1, M5SerializedLogicalTrimStoreV2 {
    public static final int MAX_NATIVE_KEY_BYTES = 512;
    private static final Pattern CELL_ROOT = Pattern.compile("/(?:[A-Za-z0-9_-]+)(?:/[A-Za-z0-9_-]+)*");
    private static final int VERSION_TOKEN_MAGIC = 0x4d354f32; // M5O2
    private final Sha256Digest routeDigest;
    private final String cellRoot;
    private final BindingIdentity binding;
    private final M4ReadControlKeysV1 keys;
    private final M5RetiredBatchHistoryV2 history;
    private final String historyPrefix;
    private final String decisionPrefix;
    private final M5RetentionKeysV1 retentionKeys;
    private final String retentionPrefix;
    private final Optional<Sha256Digest> materializationCell;
    private final Optional<MetadataNamespaceIdentityV2> kafkaMaterializationNamespace;
    private final M5MaterializationKeysV1 materializationKeys;
    private final String materializationPrefix;
    private final String cellAdmissionKey;
    private final Pattern acceptedKey;
    private final Oxia09ExactMetadataTransactionStoreV1 exact;
    private final Oxia09ExactMetadataTransactionStoreV1 nativeReferences;
    private final CanonicalControlMetadataStore rawControl;
    private final CanonicalControlMetadataStore control;
    private final java.util.Map<Sha256Digest, TrimWrite> pendingTrimWrites = new java.util.LinkedHashMap<>();

    public OxiaBindingLifecycleMetadataStoreV2(
            AsyncOxiaClient client, String cellRoot, int shardId, BindingIdentity binding) {
        this(new AsyncOxiaConditionalClient(client), cellRoot, shardId, binding);
    }

    public OxiaBindingLifecycleMetadataStoreV2(
            OxiaConditionalClient client, String cellRoot, int shardId, BindingIdentity binding) {
        this(client, cellRoot, shardId, binding, Optional.empty(), Optional.empty());
    }

    /** The actual Protocol Cell adds only its canonical M5 values and one shared Cell admission key. */
    public OxiaBindingLifecycleMetadataStoreV2(
            AsyncOxiaClient client, String cellRoot, int shardId, BindingIdentity binding, Sha256Digest protocolCell) {
        this(new AsyncOxiaConditionalClient(client), cellRoot, shardId, binding, protocolCell);
    }

    OxiaBindingLifecycleMetadataStoreV2(
            AsyncOxiaClient client,
            String cellRoot,
            int shardId,
            BindingIdentity binding,
            Sha256Digest protocolCell,
            MetadataNamespaceIdentityV2 kafkaNamespace) {
        this(
                new AsyncOxiaConditionalClient(client),
                cellRoot,
                shardId,
                binding,
                Optional.of(protocolCell),
                Optional.of(kafkaNamespace));
    }

    public OxiaBindingLifecycleMetadataStoreV2(
            OxiaConditionalClient client,
            String cellRoot,
            int shardId,
            BindingIdentity binding,
            Sha256Digest protocolCell) {
        this(client, cellRoot, shardId, binding, Optional.of(protocolCell), Optional.empty());
    }

    private OxiaBindingLifecycleMetadataStoreV2(
            OxiaConditionalClient client,
            String cellRoot,
            int shardId,
            BindingIdentity binding,
            Optional<Sha256Digest> protocolCell,
            Optional<MetadataNamespaceIdentityV2> kafkaNamespace) {
        Objects.requireNonNull(client, "client");
        this.cellRoot = Objects.requireNonNull(cellRoot, "cellRoot");
        this.binding = Objects.requireNonNull(binding, "binding");
        materializationCell = protocolCell;
        kafkaMaterializationNamespace = kafkaNamespace;
        protocolCell.ifPresent(value -> {
            if (value.isZero()) {
                throw new IllegalArgumentException("materialization Cell identity is zero");
            }
        });
        if (cellRoot.length() > MAX_NATIVE_KEY_BYTES
                || !CELL_ROOT.matcher(cellRoot).matches()) {
            throw new IllegalArgumentException("lifecycle Cell root is not canonical ASCII");
        }
        keys = new M4ReadControlKeysV1(shardId, binding);
        materializationKeys = new M5MaterializationKeysV1(shardId, binding);
        retentionKeys = new M5RetentionKeysV1(shardId, binding);
        retentionPrefix = retentionKeys
                .trimFrontier()
                .substring(0, retentionKeys.trimFrontier().lastIndexOf('/'));
        String samplePlan = materializationKeys.plan(parseSha("1".repeat(64)));
        materializationPrefix = samplePlan.substring(0, samplePlan.length() - "/plans/".length() - 64);
        cellAdmissionKey =
                protocolCell.map(M5MaterializationAdmissionV1::stateKey).orElse("");
        history = new M5RetiredBatchHistoryV2(binding);
        routeDigest = Sha256Digest.hash(CanonicalBytes.copyOf((cellRoot + "\0" + keys.selector() + "\0"
                        + binding.incarnationSha256().toHex()
                        + binding.storageEpochSha256().toHex())
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII)));
        String sampleNode = history.key(parseSha("1".repeat(64)));
        historyPrefix = sampleNode.substring(0, sampleNode.length() - 64);
        String sampleDecision = M5TaskSelectionDecisionV2.key(binding, parseSha("1".repeat(64)));
        decisionPrefix = sampleDecision.substring(0, sampleDecision.length() - 64);
        String m4Prefix = keys.selector().substring(0, keys.selector().length() - "/selector".length());
        String nativeInput = kafkaNamespace.isEmpty() ? "native-inputs|" : "";
        String retention = kafkaNamespace.isEmpty()
                ? "|" + Pattern.quote(retentionPrefix)
                        + "/(?:trim-frontier|(?:floor-snapshots|reference-free-proofs)/[0-9a-f]{64})"
                : "";
        String materialization = protocolCell.isPresent()
                ? "|" + Pattern.quote(materializationPrefix)
                        + "/(?:" + nativeInput + "source-cuts|pending-outputs|plans|tasks|"
                        + "validations|generations|manifests)/[0-9a-f]{64}|"
                        + Pattern.quote(cellAdmissionKey) + retention
                : "";
        acceptedKey = Pattern.compile("(?:" + Pattern.quote(m4Prefix)
                + "(?:/selector|/proof-head|/(?:capabilities|terminals|proofs)/[0-9]{20}"
                + "|/protections/[0-9a-f]{64}-[0-9]{20})|" + Pattern.quote(historyPrefix) + "[0-9a-f]{64}|"
                + Pattern.quote(decisionPrefix) + "[0-9a-f]{64}" + materialization + ")");
        int maximumRelative = Math.max(
                Math.max(sampleNode.length(), sampleDecision.length()),
                keys.protection(parseSha("1".repeat(64)), Long.MAX_VALUE).length());
        if (protocolCell.isPresent()) {
            maximumRelative = Math.max(
                    maximumRelative,
                    Math.max(
                            cellAdmissionKey.length(),
                            Math.max(
                                    materializationKeys
                                            .nativeInput(parseSha("1".repeat(64)))
                                            .length(),
                                    materializationKeys
                                            .pendingOutputs(parseSha("1".repeat(64)))
                                            .length())));
        }
        if (protocolCell.isPresent()) {
            maximumRelative = Math.max(
                    maximumRelative,
                    Math.max(
                            retentionKeys
                                    .floorSnapshot(parseSha("1".repeat(64)))
                                    .length(),
                            retentionKeys
                                    .referenceFreeProof(parseSha("1".repeat(64)))
                                    .length()));
        }
        if (cellRoot.length() + 1 + maximumRelative > MAX_NATIVE_KEY_BYTES) {
            throw new IllegalArgumentException(
                    "lifecycle Cell root leaves insufficient room for a 512-byte native key");
        }
        exact = new Oxia09ExactMetadataTransactionStoreV1(new RoutedClient(client));
        nativeReferences = new Oxia09ExactMetadataTransactionStoreV1(client);
        rawControl = new CanonicalView();
        control = new M5BindingAuthorityControlMetadataStoreV1(rawControl, keys.selector());
    }

    /** Direct projected-selector access; M4ReadControlCoordinatorV1 must receive rawControlMetadata instead. */
    public CanonicalControlMetadataStore controlMetadata() {
        return control;
    }

    /** Validated raw envelope view for coordinators that own their M5 selector projection. */
    public CanonicalControlMetadataStore rawControlMetadata() {
        return rawControl;
    }

    /** Checked native projection for admission diagnostics and exact backend conformance. */
    public String nativeKey(String relativeKey) {
        Objects.requireNonNull(relativeKey, "relativeKey");
        if (relativeKey.length() > MAX_NATIVE_KEY_BYTES
                || !acceptedKey.matcher(relativeKey).matches()) {
            throw new IllegalArgumentException("key is outside the exact lifecycle Binding route");
        }
        return cellRoot + "/" + relativeKey;
    }

    /** Converts only a byte/version-exact routed observation to its actual physical backend fact. */
    public CompletionStage<VersionedValue> nativeFact(VersionedValue expected) {
        String qualified =
                nativeKey(Objects.requireNonNull(expected, "expected").key());
        verifyValue(expected.key(), expected.canonicalStoredBytes());
        return nativeReferences.read(qualified).thenApply(value -> {
            var actual = value.orElseThrow(() -> new IllegalStateException("native lifecycle fact is missing"));
            if (!actual.key().equals(qualified)
                    || !VersionedValue.of(
                                    expected.key(),
                                    actual.canonicalStoredBytes(),
                                    scopedVersion(actual.metadataVersion()))
                            .equals(expected)) {
                throw new IllegalStateException("native lifecycle fact differs from its exact routed observation");
            }
            return actual;
        });
    }

    @Override
    public CompletionStage<Optional<VersionedValue>> read(String key) {
        nativeKey(key);
        if (key.equals(retentionKeys.trimFrontier())) {
            return read(keys.selector()).thenCompose(root -> {
                if (root.filter(v -> M5BindingAuthorityCodecV1.isAuthorityValue(v.canonicalStoredBytes())
                                && M5BindingAuthorityCodecV1.decodeAuthority(v.canonicalStoredBytes())
                                        .logicalTrim()
                                        .isPresent())
                        .isPresent()) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException(
                            "logical trim frontier is stored in selector authority; use readLogicalTrimFrontier"));
                }
                return exact.read(key)
                        .thenApply(observed -> observed.map(value -> VersionedValue.of(
                                key, value.canonicalStoredBytes(), scopedVersion(value.metadataVersion()))));
            });
        }
        return exact.read(key)
                .thenApply(observed -> observed.map(value ->
                        VersionedValue.of(key, value.canonicalStoredBytes(), scopedVersion(value.metadataVersion()))));
    }

    @Override
    public CompletionStage<MutationOutcome> compareAndSet(
            Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
        nativeKey(key);
        Objects.requireNonNull(predecessor, "predecessor");
        verifyValue(key, candidate);
        predecessor.ifPresent(value -> {
            if (!key.equals(value.key())) {
                throw new IllegalArgumentException("lifecycle predecessor belongs to another key");
            }
            verifyValue(key, value.canonicalStoredBytes());
        });
        if ((key.startsWith(historyPrefix) || key.startsWith(decisionPrefix)) && predecessor.isPresent()) {
            throw new IllegalArgumentException("retired history nodes are immutable create-only values");
        }
        if (key.startsWith(materializationPrefix + "/")
                && !key.startsWith(retentionPrefix + "/")
                && !key.startsWith(materializationPrefix + "/tasks/")
                && !key.startsWith(materializationPrefix + "/pending-outputs/")
                && predecessor.isPresent()) {
            throw new IllegalArgumentException("materialization immutable values are create-only");
        }
        if (key.startsWith(retentionPrefix + "/")
                && !key.equals(retentionKeys.trimFrontier())
                && predecessor.isPresent()) {
            throw new IllegalArgumentException("retention snapshots and proofs are immutable create-only values");
        }
        if (key.equals(keys.selector())) {
            requireSelectorSuccessor(predecessor, candidate);
        }
        if (key.contains("/pending-outputs/")) {
            requirePendingOutputTransition(predecessor, candidate);
        }
        var nativePredecessor = predecessor.map(
                value -> VersionedValue.of(key, value.canonicalStoredBytes(), nativeVersion(value.metadataVersion())));
        if (key.equals(retentionKeys.trimFrontier())) {
            return requireRetentionCandidate(predecessor, key, candidate)
                    .thenCompose(ignored -> CompletableFuture.failedFuture(
                            new IllegalArgumentException("logical trim must commit inside its selector authority")));
        }
        return requireBootstrapCapability(key, candidate)
                .thenCompose(ignored -> requireTaskDecisionAuthority(predecessor, key, candidate))
                .thenCompose(ignored -> requireMaterializationCandidate(key, candidate))
                .thenCompose(ignored -> requireRetentionCandidate(predecessor, key, candidate))
                .thenCompose(ignored -> key.equals(keys.selector()) || detachedImmutableValue(key)
                        ? exact.compareAndSet(nativePredecessor, key, candidate)
                        : guardedTrimWrite(predecessor, key, candidate));
    }

    /** This reads the inline frontier from its actual selector authority, never from a synthetic key/value. */
    public CompletionStage<Optional<BindingTrimFrontierV1>> readLogicalTrimFrontier() {
        return read(keys.selector()).thenCompose(value -> {
            if (value.filter(v -> M5BindingAuthorityCodecV1.isAuthorityValue(v.canonicalStoredBytes()))
                    .isPresent()) {
                var authority = M5BindingAuthorityCodecV1.decodeAuthority(
                        value.orElseThrow().canonicalStoredBytes());
                if (authority.logicalTrim().isPresent()) {
                    return CompletableFuture.completedFuture(
                            authority.logicalTrim().orElseThrow().frontier());
                }
            }
            return exact.read(retentionKeys.trimFrontier())
                    .thenApply(old -> old.map(v -> M5RetentionCodecV1.decodeTrimFrontier(v.canonicalStoredBytes())));
        });
    }

    public M5BindingLogicalTrimCoordinatorV2 logicalTrimCoordinator() {
        return new M5BindingLogicalTrimCoordinatorV2(this, keys.selector());
    }

    @Override
    public CompletionStage<M5LogicalTrimCoordinatorV1.Result> advanceLogicalTrim(
            String frontierKey, RetentionFloorSnapshotV1 snapshot) {
        if (!frontierKey.equals(retentionKeys.trimFrontier())) {
            throw new IllegalArgumentException("logical trim key belongs to another Binding route");
        }
        verifyRetentionValue(
                retentionKeys.floorSnapshot(snapshot.snapshotRootSha256()),
                M5RetentionCodecV1.encodeSnapshot(snapshot));
        return read(retentionKeys.floorSnapshot(snapshot.snapshotRootSha256()))
                .thenCompose(stored -> {
                    if (stored.filter(v -> v.canonicalStoredBytes().equals(M5RetentionCodecV1.encodeSnapshot(snapshot)))
                            .isEmpty()) {
                        throw new IllegalArgumentException("logical trim lacks its exact stored floor snapshot");
                    }
                    return migrateLegacyTrim().thenCompose(ignored -> read(keys.selector()));
                })
                .thenCompose(value -> {
                    var open = value.filter(v -> M5BindingAuthorityCodecV1.isAuthorityValue(v.canonicalStoredBytes()));
                    if (open.isEmpty()) {
                        return CompletableFuture.completedFuture(
                                trimResult(M5LogicalTrimCoordinatorV1.Outcome.CONFLICT));
                    }
                    var exact = open.orElseThrow();
                    var authority = M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
                    var prior = authority.logicalTrim().flatMap(LogicalTrimStateV2::frontier);
                    if (prior.filter(v -> M5LogicalTrimCoordinatorV1.exactRetry(v, snapshot))
                            .isPresent()) {
                        return CompletableFuture.completedFuture(new M5LogicalTrimCoordinatorV1.Result(
                                M5LogicalTrimCoordinatorV1.Outcome.EXISTING_EXACT, prior));
                    }
                    var attempt = Sha256Digest.hash(CanonicalUtf8.fromString("NEREUS_BINDING_TRIM_V2:" + keys.selector()
                                    + ":" + snapshot.snapshotRootSha256().toHex())
                            .bytes());
                    var coordinator = logicalTrimCoordinator();
                    var fence = authority.logicalTrim().flatMap(LogicalTrimStateV2::fence);
                    CompletionStage<M5BindingLogicalTrimCoordinatorV2.Result> work;
                    if (fence.filter(f -> f.attemptIdSha256().equals(attempt)
                                    && f.floorSnapshotRootSha256().equals(snapshot.snapshotRootSha256()))
                            .isPresent()) {
                        work = coordinator.commit(exact, snapshot, attempt);
                    } else {
                        work = coordinator
                                .fence(exact, snapshot, attempt)
                                .thenCompose(fenced -> fenced.exactAuthority().isPresent()
                                        ? coordinator.commit(
                                                fenced.exactAuthority().orElseThrow(), snapshot, attempt)
                                        : CompletableFuture.completedFuture(fenced));
                    }
                    return work.thenApply(result -> {
                        var outcome =
                                switch (result.outcome()) {
                                    case APPLIED_EXACT -> M5LogicalTrimCoordinatorV1.Outcome.APPLIED_EXACT;
                                    case EXISTING_EXACT -> M5LogicalTrimCoordinatorV1.Outcome.EXISTING_EXACT;
                                    case DEFINITIVELY_NOT_APPLIED ->
                                        M5LogicalTrimCoordinatorV1.Outcome.DEFINITIVELY_NOT_APPLIED;
                                    case RESPONSE_UNKNOWN -> M5LogicalTrimCoordinatorV1.Outcome.RESPONSE_UNKNOWN;
                                    case RETAIN, CONFLICT -> M5LogicalTrimCoordinatorV1.Outcome.CONFLICT;
                                };
                        return result.exactAuthority().isPresent()
                                ? new M5LogicalTrimCoordinatorV1.Result(
                                        outcome,
                                        M5BindingAuthorityCodecV1.decodeAuthority(result.exactAuthority()
                                                        .orElseThrow()
                                                        .canonicalStoredBytes())
                                                .logicalTrim()
                                                .orElseThrow()
                                                .frontier())
                                : trimResult(outcome);
                    });
                });
    }

    private CompletionStage<Void> migrateLegacyTrim() {
        return read(keys.selector()).thenCompose(value -> {
            if (value.filter(v -> M5BindingAuthorityCodecV1.isAuthorityValue(v.canonicalStoredBytes()))
                    .isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            var exactAuthority = value.orElseThrow();
            var authority = M5BindingAuthorityCodecV1.decodeAuthority(exactAuthority.canonicalStoredBytes());
            if (authority.logicalTrim().isPresent()) {
                return CompletableFuture.completedFuture(null);
            }
            return exact.read(retentionKeys.trimFrontier()).thenCompose(legacy -> {
                if (legacy.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }
                if (authority.state() != BindingAuthorityStateV1.OPEN_V1
                        || authority.writerEnrollment().isEmpty()
                        || !authority.referenceMutationTickets().isEmpty()) {
                    throw new IllegalStateException(
                            "legacy trim migration requires quiescent enrolled Binding writers");
                }
                var trim = M5RetentionCodecV1.decodeTrimFrontier(
                        legacy.orElseThrow().canonicalStoredBytes());
                var candidate = M5BindingAuthorityCodecV1.logicalTrimSuccessor(
                        authority, new LogicalTrimStateV2(Optional.of(trim), Optional.empty()), java.util.List.of());
                return compareAndSet(
                                Optional.of(exactAuthority),
                                keys.selector(),
                                M5BindingAuthorityCodecV1.encodeAuthority(candidate))
                        .thenApply(ignored -> null);
            });
        });
    }

    private static M5LogicalTrimCoordinatorV1.Result trimResult(M5LogicalTrimCoordinatorV1.Outcome outcome) {
        return new M5LogicalTrimCoordinatorV1.Result(outcome, Optional.empty());
    }

    private boolean detachedImmutableValue(String key) {
        // These immutable, content-addressed bytes acquire no reader/task authority until their root is published.
        return key.startsWith(retentionPrefix + "/floor-snapshots/")
                || key.startsWith(retentionPrefix + "/reference-free-proofs/")
                || key.startsWith(historyPrefix)
                || key.startsWith(decisionPrefix)
                || key.startsWith(materializationPrefix + "/")
                        && !key.startsWith(retentionPrefix + "/")
                        && !key.startsWith(materializationPrefix + "/tasks/")
                        && !key.startsWith(materializationPrefix + "/pending-outputs/");
    }

    private CompletionStage<MutationOutcome> guardedTrimWrite(
            Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
        return read(keys.selector()).thenCompose(root -> {
            if (root.filter(value -> M5BindingBootstrapV1.isBootstrap(value.canonicalStoredBytes()))
                    .isPresent()) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            if (root.filter(v -> M5BindingAuthorityCodecV1.isAuthorityValue(v.canonicalStoredBytes()))
                    .isEmpty()) {
                return exact.compareAndSet(toNative(predecessor), key, candidate);
            }
            var authority =
                    M5BindingAuthorityCodecV1.decodeAuthority(root.orElseThrow().canonicalStoredBytes());
            if (authority.writerEnrollment().isEmpty()) {
                return exact.compareAndSet(toNative(predecessor), key, candidate);
            }
            if (authority.state() != BindingAuthorityStateV1.OPEN_V1
                    || authority.trimMutationTickets().stream()
                            .anyMatch(ticket -> ticket.key().equals(key))) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            var operation = new LogicalTrimMutationV2(
                    Sha256Digest.hash(
                            CanonicalUtf8.fromString(java.util.UUID.randomUUID().toString())
                                    .bytes()),
                    key,
                    predecessor.map(VersionedValue::metadataVersion),
                    predecessor.map(VersionedValue::canonicalStoredSha256),
                    Sha256Digest.hash(candidate));
            var actor = new TrimWrite(operation, root.orElseThrow().metadataVersion());
            var tickets = new java.util.ArrayList<>(authority.trimMutationTickets());
            tickets.add(operation);
            tickets.sort(java.util.Comparator.comparing(LogicalTrimMutationV2::key));
            var admitted = M5BindingAuthorityCodecV1.logicalTrimSuccessor(
                    authority,
                    authority.logicalTrim().orElse(new LogicalTrimStateV2(Optional.empty(), Optional.empty())),
                    tickets);
            var admittedBytes = M5BindingAuthorityCodecV1.encodeAuthority(admitted);
            synchronized (pendingTrimWrites) {
                if (pendingTrimWrites.size() >= M5LogicalTrimRecordsV2.MAX_MUTATIONS) {
                    throw new IllegalStateException("Binding trim writer admission is full");
                }
                pendingTrimWrites.put(operation.operationId(), actor);
            }
            var work = compareAndSet(root, keys.selector(), admittedBytes)
                    .thenCompose(ignored -> read(keys.selector()))
                    .thenCompose(observed -> {
                        if (observed.filter(v -> M5BindingAuthorityCodecV1.decodeAuthority(v.canonicalStoredBytes())
                                        .trimMutationTickets()
                                        .contains(operation))
                                .isEmpty()) {
                            return settleUndispatched(actor)
                                    .thenApply(settled -> settled
                                            ? MutationOutcome.DEFINITIVE_CONFLICT
                                            : MutationOutcome.RESPONSE_UNKNOWN);
                        }
                        actor.dispatched = true;
                        return exact.compareAndSet(toNative(predecessor), key, candidate)
                                .thenCompose(outcome -> {
                                    if (outcome == MutationOutcome.PREDECESSOR_UNCHANGED
                                            || outcome == MutationOutcome.DEFINITIVE_CONFLICT) {
                                        return clearTrimWrite(operation)
                                                .thenApply(cleared ->
                                                        cleared ? outcome : MutationOutcome.RESPONSE_UNKNOWN);
                                    }
                                    return read(key).thenCompose(current -> {
                                        boolean applied = current.filter(v -> v.canonicalStoredSha256()
                                                                .equals(operation.candidateSha256())
                                                        && operation
                                                                .predecessorVersion()
                                                                .filter(v.metadataVersion()::equals)
                                                                .isEmpty())
                                                .isPresent();
                                        return applied
                                                ? clearTrimWrite(operation)
                                                        .thenApply(cleared -> cleared
                                                                ? MutationOutcome.APPLIED_EXACT
                                                                : MutationOutcome.RESPONSE_UNKNOWN)
                                                : CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
                                    });
                                });
                    })
                    .exceptionally(ignored -> MutationOutcome.RESPONSE_UNKNOWN)
                    .toCompletableFuture();
            return work.copy();
        });
    }

    private Optional<VersionedValue> toNative(Optional<VersionedValue> values) {
        return values.map(
                v -> VersionedValue.of(v.key(), v.canonicalStoredBytes(), nativeVersion(v.metadataVersion())));
    }

    private CompletionStage<Boolean> clearTrimWrite(LogicalTrimMutationV2 ticket) {
        return read(keys.selector()).thenCompose(value -> {
            var original = value.orElseThrow();
            var authority = M5BindingAuthorityCodecV1.decodeAuthority(original.canonicalStoredBytes());
            if (!authority.trimMutationTickets().contains(ticket)) {
                synchronized (pendingTrimWrites) {
                    pendingTrimWrites.remove(ticket.operationId());
                }
                return CompletableFuture.completedFuture(true);
            }
            if (authority.state() != BindingAuthorityStateV1.OPEN_V1) {
                return CompletableFuture.completedFuture(false);
            }
            var next = M5BindingAuthorityCodecV1.logicalTrimSuccessor(
                    authority,
                    authority.logicalTrim().orElseThrow(),
                    authority.trimMutationTickets().stream()
                            .filter(t -> !t.equals(ticket))
                            .toList());
            return compareAndSet(
                            Optional.of(original), keys.selector(), M5BindingAuthorityCodecV1.encodeAuthority(next))
                    .thenCompose(ignored -> read(keys.selector()))
                    .thenApply(current -> {
                        boolean cleared = current.filter(
                                        v -> M5BindingAuthorityCodecV1.decodeAuthority(v.canonicalStoredBytes())
                                                .trimMutationTickets()
                                                .contains(ticket))
                                .isEmpty();
                        if (cleared && current.isPresent()) {
                            synchronized (pendingTrimWrites) {
                                pendingTrimWrites.remove(ticket.operationId());
                            }
                            return true;
                        }
                        return false;
                    });
        });
    }

    private CompletionStage<Boolean> settleUndispatched(TrimWrite actor) {
        return read(keys.selector()).thenCompose(value -> {
            var original = value.orElseThrow();
            var authority = M5BindingAuthorityCodecV1.decodeAuthority(original.canonicalStoredBytes());
            if (authority.trimMutationTickets().contains(actor.ticket)) {
                return clearTrimWrite(actor.ticket);
            }
            if (!original.metadataVersion().equals(actor.admissionPredecessor)) {
                synchronized (pendingTrimWrites) {
                    pendingTrimWrites.remove(actor.ticket.operationId());
                }
                return CompletableFuture.completedFuture(true);
            }
            if (authority.state() != BindingAuthorityStateV1.OPEN_V1) {
                return CompletableFuture.completedFuture(false);
            }
            var barrier = M5BindingAuthorityCodecV1.logicalTrimSuccessor(
                    authority,
                    authority.logicalTrim().orElse(new LogicalTrimStateV2(Optional.empty(), Optional.empty())),
                    authority.trimMutationTickets());
            return compareAndSet(
                            Optional.of(original), keys.selector(), M5BindingAuthorityCodecV1.encodeAuthority(barrier))
                    .thenCompose(ignored -> read(keys.selector()))
                    .thenApply(after -> {
                        boolean settled = after.filter(v -> !v.metadataVersion().equals(actor.admissionPredecessor)
                                        && !M5BindingAuthorityCodecV1.decodeAuthority(v.canonicalStoredBytes())
                                                .trimMutationTickets()
                                                .contains(actor.ticket))
                                .isPresent();
                        if (settled) {
                            synchronized (pendingTrimWrites) {
                                pendingTrimWrites.remove(actor.ticket.operationId());
                            }
                        }
                        return settled;
                    });
        });
    }

    /** Reconciles only original writes or exact version barriers; it never redispatches a candidate payload. */
    public CompletionStage<Void> reconcileLogicalTrimWrites() {
        return read(keys.selector()).thenCompose(value -> {
            var tickets = new java.util.LinkedHashMap<Sha256Digest, LogicalTrimMutationV2>();
            value.filter(v -> M5BindingAuthorityCodecV1.isAuthorityValue(v.canonicalStoredBytes()))
                    .ifPresent(v -> M5BindingAuthorityCodecV1.decodeAuthority(v.canonicalStoredBytes())
                            .trimMutationTickets()
                            .forEach(t -> tickets.put(t.operationId(), t)));
            synchronized (pendingTrimWrites) {
                pendingTrimWrites.values().forEach(actor -> tickets.put(actor.ticket.operationId(), actor.ticket));
            }
            CompletionStage<Void> stage = CompletableFuture.completedFuture(null);
            for (var ticket : tickets.values()) {
                stage = stage.thenCompose(ignored -> reconcileTrimWrite(ticket).thenApply(settled -> {
                    if (!settled) {
                        throw new IllegalStateException("original logical trim writer remains unresolved");
                    }
                    return null;
                }));
            }
            return stage;
        });
    }

    private CompletionStage<Boolean> reconcileTrimWrite(LogicalTrimMutationV2 ticket) {
        final TrimWrite actor;
        synchronized (pendingTrimWrites) {
            actor = pendingTrimWrites.get(ticket.operationId());
        }
        if (actor != null && !actor.dispatched) {
            return settleUndispatched(actor);
        }
        return read(ticket.key()).thenCompose(current -> {
            if (current.isPresent()
                    && ticket.predecessorVersion()
                            .filter(current.orElseThrow().metadataVersion()::equals)
                            .isEmpty()) {
                return clearTrimWrite(ticket);
            }
            if (current.isEmpty() || ticket.predecessorVersion().isEmpty()) {
                return CompletableFuture.completedFuture(false);
            }
            var original = current.orElseThrow();
            // A same-value CAS advances the native version and excludes the original conditional request.
            return exact.compareAndSet(toNative(current), ticket.key(), original.canonicalStoredBytes())
                    .thenCompose(ignored -> read(ticket.key()))
                    .thenCompose(after -> after.filter(v -> !v.metadataVersion()
                                            .equals(ticket.predecessorVersion().orElseThrow()))
                                    .isPresent()
                            ? clearTrimWrite(ticket)
                            : CompletableFuture.completedFuture(false));
        });
    }

    private static final class TrimWrite {
        private final LogicalTrimMutationV2 ticket;
        private final MetadataVersion admissionPredecessor;
        private volatile boolean dispatched;

        private TrimWrite(LogicalTrimMutationV2 ticket, MetadataVersion admissionPredecessor) {
            this.ticket = ticket;
            this.admissionPredecessor = admissionPredecessor;
        }
    }

    @Override
    public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
        Objects.requireNonNull(transaction, "transaction");
        return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
    }

    @Override
    public boolean supportsAtomicMultiKeyTransactions() {
        return false;
    }

    private MetadataVersion scopedVersion(MetadataVersion nativeVersion) {
        byte[] original = nativeVersion.value().toByteArray();
        if (original.length != Long.BYTES) {
            throw new IllegalStateException("native lifecycle version is not the locked Oxia version format");
        }
        return new MetadataVersion(CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(44)
                .putInt(VERSION_TOKEN_MAGIC)
                .put(routeDigest.bytes().toByteArray())
                .put(original)
                .array()));
    }

    private MetadataVersion nativeVersion(MetadataVersion scopedVersion) {
        byte[] token = scopedVersion.value().toByteArray();
        if (token.length != 44) {
            throw new IllegalArgumentException("lifecycle version token is not scoped to this route");
        }
        var input = java.nio.ByteBuffer.wrap(token);
        int magic = input.getInt();
        byte[] digest = new byte[Sha256Digest.LENGTH];
        input.get(digest);
        if (magic != VERSION_TOKEN_MAGIC || !routeDigest.equals(Sha256Digest.copyOf(digest))) {
            throw new IllegalArgumentException("lifecycle version token belongs to another route");
        }
        byte[] nativeBytes = new byte[Long.BYTES];
        input.get(nativeBytes);
        return new MetadataVersion(CanonicalBytes.copyOf(nativeBytes));
    }

    private static Sha256Digest parseSha(String hex) {
        return Sha256Digest.copyOf(java.util.HexFormat.of().parseHex(hex));
    }

    private void verifyValue(String key, CanonicalBytes value) {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty() || value.length() > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("lifecycle value exceeds its native admission bound");
        }
        if (key.equals(cellAdmissionKey)) {
            M5MaterializationAdmissionV1.decode(value);
            return;
        }
        if (key.startsWith(retentionPrefix + "/")) {
            verifyRetentionValue(key, value);
            return;
        }
        if (key.startsWith(materializationPrefix + "/")) {
            verifyMaterializationValue(key, value);
            return;
        }
        if (key.startsWith(historyPrefix)) {
            history.verifyNode(parseSha(key.substring(historyPrefix.length())), value);
            return;
        }
        if (key.startsWith(decisionPrefix)) {
            var decision = M5TaskSelectionDecisionV2.decode(value);
            if (!decision.binding().equals(binding) || !decision.key().equals(key)) {
                throw new IllegalArgumentException("task selection archive Binding or key differs");
            }
            return;
        }
        BindingIdentity observed;
        String actualKey;
        if (key.equals(keys.selector())) {
            observed = M5BindingBootstrapV1.isBootstrap(value)
                    ? M5BindingBootstrapV1.decode(value).binding()
                    : M5BindingAuthorityCodecV1.projectSelector(value).binding();
            actualKey = keys.selector();
        } else if (key.equals(keys.proofHead())) {
            observed = M4ReadControlCodecV1.decodeHead(value).binding();
            actualKey = keys.proofHead();
        } else if (key.contains("/capabilities/")) {
            var record = M4ReadControlCodecV1.decodeCapability(value);
            observed = record.binding();
            actualKey = keys.capability(record.generation());
        } else if (key.contains("/terminals/")) {
            var record = M4ReadControlCodecV1.decodeTerminal(value);
            observed = record.binding();
            actualKey = keys.terminal(record.readAdmissionEpoch());
        } else if (key.contains("/proofs/")) {
            var record = M4ReadControlCodecV1.decodeProof(value);
            observed = record.binding();
            actualKey = keys.proof(record.readAdmissionEpoch());
        } else {
            var record = M4ReadControlCodecV1.decodeProtection(value);
            observed = record.binding();
            actualKey = keys.protection(
                    record.identity().sourceIdentitySha256(), record.identity().protectionGeneration());
        }
        if (!binding.equals(observed) || !key.equals(actualKey)) {
            throw new IllegalArgumentException("lifecycle record Binding, incarnation, storage epoch or key differs");
        }
    }

    private void verifyRetentionValue(String key, CanonicalBytes value) {
        IdentityEnvelope identity;
        String actual;
        if (key.equals(retentionKeys.trimFrontier())) {
            var trim = M5RetentionCodecV1.decodeTrimFrontier(value);
            identity = trim.identity();
            actual = retentionKeys.trimFrontier();
        } else if (key.startsWith(retentionPrefix + "/floor-snapshots/")) {
            var snapshot = M5RetentionCodecV1.decodeSnapshot(value);
            identity = snapshot.identity();
            actual = retentionKeys.floorSnapshot(snapshot.snapshotRootSha256());
        } else {
            var proof = M5RetentionCodecV1.decodeReferenceFreeProof(value);
            identity = proof.identity();
            actual = retentionKeys.referenceFreeProof(proof.proofSha256());
        }
        if (!identity.binding().equals(binding)
                || !materializationCell.equals(Optional.of(identity.protocolCellSha256()))
                || !key.equals(actual)) {
            throw new IllegalArgumentException("retention Binding/Cell/identity or content-addressed key differs");
        }
    }

    private CompletionStage<Void> requireRetentionCandidate(
            Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
        if (!key.equals(retentionKeys.trimFrontier())) {
            return CompletableFuture.completedFuture(null);
        }
        var trim = M5RetentionCodecV1.decodeTrimFrontier(candidate);
        return read(retentionKeys.floorSnapshot(trim.floorSnapshotRootSha256())).thenCompose(stored -> {
            var snapshot = M5RetentionCodecV1.decodeSnapshot(stored.orElseThrow(() ->
                            new IllegalArgumentException("trim frontier lacks its exact immutable floor snapshot"))
                    .canonicalStoredBytes());
            var previous =
                    predecessor.map(value -> M5RetentionCodecV1.decodeTrimFrontier(value.canonicalStoredBytes()));
            if (!new M5RetentionPlannerV1().plan(snapshot, previous).equals(trim)) {
                throw new IllegalArgumentException("trim frontier differs from the exact floor minimum/predecessor");
            }
            return new M5ReferenceFreshnessVerifierV1(this)
                    .requireFresh(snapshot)
                    .thenApply(ignored -> (Void) null);
        });
    }

    private void requireMaterializationIdentity(IdentityEnvelope identity) {
        if (!identity.binding().equals(binding)
                || !materializationCell.equals(Optional.of(identity.protocolCellSha256()))) {
            throw new IllegalArgumentException("materialization Binding/Cell/incarnation/StorageEpoch differs");
        }
    }

    private void verifyMaterializationValue(String key, CanonicalBytes value) {
        String actual;
        if (key.contains("/pending-outputs/")) {
            var inventory = com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(value);
            requireMaterializationIdentity(inventory.worker().identity());
            actual = materializationKeys.pendingOutputs(inventory.worker().taskIdSha256());
        } else if (key.contains("/native-inputs/")) {
            var input = OxiaPulsarMaterializationInputV2.decode(value);
            requireNativeInputRoute(input);
            actual = materializationKeys.nativeInput(input.protocolStateRootSha256());
        } else if (key.contains("/source-cuts/")) {
            var cut = M5MaterializationCodecV1.decodeSourceCut(value);
            requireMaterializationIdentity(cut.identity());
            actual = materializationKeys.sourceCut(M5MaterializationCodecV1.calculateTaskId(cut));
        } else if (key.contains("/plans/")) {
            var plan = M5MaterializationCodecV1.decodePlan(value);
            requireMaterializationIdentity(plan.sourceCut().identity());
            actual = materializationKeys.plan(plan.taskIdSha256());
        } else if (key.contains("/tasks/")) {
            actual = materializationKeys.task(
                    M5MaterializationCodecV1.decodeTask(value).taskIdSha256());
        } else if (key.contains("/validations/")) {
            actual = materializationKeys.validation(M5MaterializationCodecV1.validationRootSha256(
                    M5MaterializationCodecV1.decodeValidationRoot(value)));
        } else if (key.contains("/generations/")) {
            var generation = M5MaterializationCodecV1.decodeGeneration(value);
            requireMaterializationIdentity(generation.identity());
            actual = materializationKeys.generation(M5MaterializationCodecV1.generationSha256(generation));
        } else {
            var manifest = M5MaterializationCodecV1.decodeManifest(value);
            requireMaterializationIdentity(manifest.identity());
            actual = materializationKeys.manifest(M5MaterializationCodecV1.manifestSha256(manifest));
        }
        if (!actual.equals(key)) {
            throw new IllegalArgumentException("materialization canonical key/body digest differs");
        }
    }

    private CompletionStage<Void> requireMaterializationCandidate(String key, CanonicalBytes candidate) {
        if (!key.startsWith(materializationPrefix + "/") || key.startsWith(retentionPrefix + "/")) {
            return CompletableFuture.completedFuture(null);
        }
        if (key.contains("/native-inputs/")) {
            return requireNativeInputReferences(OxiaPulsarMaterializationInputV2.decode(candidate));
        }
        if (key.contains("/source-cuts/")) {
            var cut = M5MaterializationCodecV1.decodeSourceCut(candidate);
            if (!cut.coverage().typedPulsar()) {
                return CompletableFuture.completedFuture(null);
            }
            return read(materializationKeys.nativeInput(cut.protocolStateRootSha256()))
                    .thenCompose(stored -> {
                        var input = OxiaPulsarMaterializationInputV2.decode(
                                stored.orElseThrow(() -> new IllegalArgumentException(
                                                "Native source cut lacks its complete stored input"))
                                        .canonicalStoredBytes());
                        if (!input.coverage()
                                        .equals(cut.coverage().pulsarCoverage().orElseThrow())
                                || !input.recoveryInputSha256().equals(cut.recoveryCheckpointRootSha256())
                                || !input.coverage().chainAuthorityRootSha256().equals(cut.predecessorViewSha256())
                                || input.storageFence() != cut.identity().storageFence()) {
                            throw new IllegalArgumentException(
                                    "Native cut/input coverage, recovery or Storage Epoch differs");
                        }
                        return requireNativeInputReferences(input);
                    });
        }
        if (key.contains("/plans/")) {
            var plan = M5MaterializationCodecV1.decodePlan(candidate);
            return read(materializationKeys.sourceCut(plan.taskIdSha256())).thenAccept(cut -> {
                if (!cut.map(VersionedValue::canonicalStoredBytes)
                        .equals(Optional.of(M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut())))) {
                    throw new IllegalArgumentException("complete plan lacks its exact immutable source cut");
                }
            });
        }
        Sha256Digest task;
        if (key.contains("/pending-outputs/")) {
            task = com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(candidate)
                    .worker()
                    .taskIdSha256();
        } else if (key.contains("/tasks/")) {
            task = M5MaterializationCodecV1.decodeTask(candidate).taskIdSha256();
        } else if (key.contains("/validations/")) {
            task = M5MaterializationCodecV1.decodeValidationRoot(candidate).taskIdSha256();
        } else if (key.contains("/generations/")) {
            task = M5MaterializationCodecV1.decodeGeneration(candidate).taskIdSha256();
        } else {
            var manifest = M5MaterializationCodecV1.decodeManifest(candidate);
            return read(materializationKeys.generation(manifest.preferredGenerationSha256()))
                    .thenAccept(generation -> {
                        var exactGeneration = M5MaterializationCodecV1.decodeGeneration(generation
                                .orElseThrow(
                                        () -> new IllegalArgumentException("manifest has no exact stored generation"))
                                .canonicalStoredBytes());
                        if (!exactGeneration.coverage().equals(manifest.coverage())
                                || !exactGeneration.identity().equals(manifest.identity())) {
                            throw new IllegalArgumentException("manifest generation scope/coverage differs");
                        }
                    });
        }
        return read(materializationKeys.plan(task)).thenCompose(stored -> {
            var plan = M5MaterializationCodecV1.decodePlan(stored.orElseThrow(
                            () -> new IllegalArgumentException("materialization value has no complete stored plan"))
                    .canonicalStoredBytes());
            requirePlanCandidate(plan, key, candidate);
            if (key.contains("/pending-outputs/")) {
                return requirePendingOutputProfile(plan, M5PendingOutputInventoryV2.decode(candidate));
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    private CompletionStage<Void> requirePendingOutputProfile(
            MaterializationPlan plan, M5PendingOutputInventoryV2 inventory) {
        var profile = inventory.profile();
        var namespace = profile.physicalBinding().metadataNamespace();
        CompletionStage<Void> exactInput;
        if (kafkaMaterializationNamespace.isPresent()) {
            if (plan.sourceCut().coverage().domain() != PositionDomain.KAFKA_OFFSET
                    || !kafkaMaterializationNamespace.orElseThrow().equals(namespace)
                    || !inventory
                            .worker()
                            .nativeNamespaceBindingSha256()
                            .equals(Sha256Digest.hash(profile.physicalBinding().encode()))) {
                throw new IllegalArgumentException("Kafka pending output differs from its native Binding or namespace");
            }
            exactInput = inventory.state() == M5PendingOutputInventoryV2.State.PREPARED
                    ? read(keys.selector()).thenAccept(stored -> {
                        var authority = M5BindingAuthorityCodecV1.decodeAuthority(stored.orElseThrow(() ->
                                        new IllegalArgumentException("Kafka pending output lacks its worker authority"))
                                .canonicalStoredBytes());
                        if (authority.state() != BindingAuthorityStateV1.OPEN_V1
                                || !authority
                                        .selectorProjection()
                                        .equals(plan.sourceCut().predecessorSelector())
                                || !authority.materializationWorker().equals(Optional.of(inventory.worker()))) {
                            throw new IllegalArgumentException("Kafka pending output worker is stale");
                        }
                    })
                    : CompletableFuture.completedFuture(null);
        } else {
            exactInput = read(materializationKeys.nativeInput(plan.sourceCut().protocolStateRootSha256()))
                    .thenAccept(input -> {
                        var nativeInput = OxiaPulsarMaterializationInputV2.decode(
                                input.orElseThrow(() -> new IllegalArgumentException(
                                                "pending outputs lack their complete Native input"))
                                        .canonicalStoredBytes());
                        if (!namespace.equals(nativeInput.metadataNamespace())) {
                            throw new IllegalArgumentException("pending output profile metadata namespace differs");
                        }
                    });
        }
        return exactInput
                .thenCompose(ignored ->
                        nativeReferences.read(profile.physicalBinding().authorityRoot() + "/cell-provider-scopes-v2"))
                .thenAccept(catalogue -> {
                    if (!OxiaCellProviderScopeRegistryV2.Catalogue.decode(catalogue
                                    .orElseThrow(() ->
                                            new IllegalArgumentException("pending outputs lack their admitted profile"))
                                    .canonicalStoredBytes())
                            .profiles()
                            .contains(profile)) {
                        throw new IllegalArgumentException("pending output profile is not actually admitted");
                    }
                });
    }

    private static void requirePendingOutputTransition(Optional<VersionedValue> before, CanonicalBytes candidate) {
        var next = com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(candidate);
        if (before.isEmpty()) {
            if (next.state()
                    != com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.State.PREPARED) {
                throw new IllegalArgumentException("pending outputs cannot import a terminal inventory");
            }
            return;
        }
        var original = com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(
                before.orElseThrow().canonicalStoredBytes());
        if (!next.equals(original)
                && (original.state()
                                != com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.State
                                        .PREPARED
                        || !next.equals(original.cancelUnstarted()))) {
            throw new IllegalArgumentException("pending outputs can only cancel the same original unstarted inventory");
        }
    }

    private void requireNativeInputRoute(OxiaPulsarMaterializationInputV2 input) {
        String selector = nativeKey(keys.selector());
        String sourcePrefix = selector.substring(0, selector.length() - "/selector".length()) + "/native-source-maps/";
        if (!input.binding().equals(binding)
                || !materializationCell.equals(Optional.of(input.protocolCellSha256()))
                || !input.sourceViewKey()
                        .equals(sourcePrefix
                                + input.coverage().chainAuthorityRootSha256().toHex())) {
            throw new IllegalArgumentException("Native input Binding/Cell/source-view route differs");
        }
    }

    private CompletionStage<Void> requireNativeInputReferences(OxiaPulsarMaterializationInputV2 input) {
        requireNativeInputRoute(input);
        return nativeReferences.read(input.aggregateKey()).thenCompose(aggregate -> nativeReferences
                .read(input.selectorKey())
                .thenCompose(selector -> {
                    input.requireCurrentP1(aggregate.orElseThrow(), selector.orElseThrow());
                    return nativeReferences
                            .read(input.sourceViewKey())
                            .thenAccept(view -> input.verifySourceView(view.orElseThrow()));
                }));
    }

    private static void requirePlanCandidate(MaterializationPlan plan, String key, CanonicalBytes candidate) {
        if (key.contains("/pending-outputs/")) {
            com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(candidate)
                    .requirePlan(plan);
        } else if (key.contains("/tasks/")) {
            var task = M5MaterializationCodecV1.decodeTask(candidate);
            if (!task.outputIdentitySha256().equals(plan.outputIdentitySha256())
                    || !task.sourceCutSha256().equals(M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()))) {
                throw new IllegalArgumentException("task differs from its complete stored plan");
            }
        } else if (key.contains("/validations/")) {
            var validation = M5MaterializationCodecV1.decodeValidationRoot(candidate);
            if (!validation.outputIdentitySha256().equals(plan.outputIdentitySha256())
                    || !validation.sourceSetSha256().equals(plan.sourceCut().sourceSetSha256())) {
                throw new IllegalArgumentException("validation differs from its complete stored plan");
            }
        } else {
            var generation = M5MaterializationCodecV1.decodeGeneration(candidate);
            if (!generation.outputIdentitySha256().equals(plan.outputIdentitySha256())
                    || !generation.sourceSetSha256().equals(plan.sourceCut().sourceSetSha256())
                    || !generation.coverage().equals(plan.sourceCut().coverage())) {
                throw new IllegalArgumentException("generation differs from its complete stored plan");
            }
        }
    }

    private CompletionStage<Void> requireBootstrapCapability(String key, CanonicalBytes candidate) {
        if (!key.equals(keys.selector())) {
            return CompletableFuture.completedFuture(null);
        }
        Optional<M5BindingBootstrapV1> bootstrap = M5BindingBootstrapV1.isBootstrap(candidate)
                ? Optional.of(M5BindingBootstrapV1.decode(candidate))
                : M5BindingAuthorityCodecV1.isAuthorityValue(candidate)
                        ? M5BindingAuthorityCodecV1.decodeAuthority(candidate).bootstrap()
                        : Optional.empty();
        if (bootstrap.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        var capability = bootstrap.orElseThrow().capability();
        return read(keys.capability(capability.generation())).thenApply(stored -> {
            var evidence = M4ReadControlCodecV1.decodeCapability(
                    stored.orElseThrow(() -> new IllegalArgumentException("Binding bootstrap capability is missing"))
                            .canonicalStoredBytes());
            if (!M4ReadControlCodecV1.capabilityEvidenceSha256(evidence).equals(capability.evidenceSha256())
                    || evidence.state()
                            != com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState
                                    .ADMITTED) {
                throw new IllegalArgumentException("Binding bootstrap capability is not exact and admitted");
            }
            return null;
        });
    }

    private void requireSelectorSuccessor(Optional<VersionedValue> predecessor, CanonicalBytes candidate) {
        if (M5BindingBootstrapV1.isBootstrap(candidate)) {
            if (predecessor.isPresent()) {
                throw new IllegalArgumentException("Binding bootstrap is create-only");
            }
            return;
        }
        if (predecessor
                .filter(value -> M5BindingBootstrapV1.isBootstrap(value.canonicalStoredBytes()))
                .isPresent()) {
            var bootstrap =
                    M5BindingBootstrapV1.decode(predecessor.orElseThrow().canonicalStoredBytes());
            var next = M5BindingAuthorityCodecV1.decodeAuthority(candidate);
            var expected = M5BindingAuthorityCodecV1.activateBootstrap(bootstrap, next.selectorProjection());
            if (!M5BindingAuthorityCodecV1.encodeAuthority(expected).equals(candidate)) {
                throw new IllegalArgumentException("Binding bootstrap activation differs from its exact successor");
            }
            return;
        }
        if (predecessor.isEmpty()) {
            if (M5BindingAuthorityCodecV1.isAuthorityValue(candidate)) {
                var next = M5BindingAuthorityCodecV1.decodeAuthority(candidate);
                if (next.authorityGeneration() != 1
                        || next.predecessorValueSha256().isPresent()
                        || next.retiredHistory().count() != 0
                        || next.taskSelectionDecision().isPresent()
                        || next.materializationWorker().isPresent()
                        || next.logicalTrim().isPresent()
                        || !next.trimMutationTickets().isEmpty()
                        || next.bootstrap().isPresent()) {
                    throw new IllegalArgumentException("lifecycle creation cannot import a prior selector history");
                }
            }
            return;
        }
        var before = predecessor.orElseThrow();
        boolean wasAuthority = M5BindingAuthorityCodecV1.isAuthorityValue(before.canonicalStoredBytes());
        if (!M5BindingAuthorityCodecV1.isAuthorityValue(candidate)) {
            if (wasAuthority) {
                throw new IllegalArgumentException("lifecycle selector cannot downgrade to legacy bytes");
            }
            return;
        }
        var next = M5BindingAuthorityCodecV1.decodeAuthority(candidate);
        long expectedGeneration = 1;
        if (wasAuthority) {
            var previous = M5BindingAuthorityCodecV1.decodeAuthority(before.canonicalStoredBytes());
            expectedGeneration = Math.addExact(previous.authorityGeneration(), 1);
            M5BindingAuthorityCodecV1.requireTaskSelectionTransition(previous, next);
            M5BindingAuthorityCodecV1.requireWorkerTransition(previous, next);
            M5BindingAuthorityCodecV1.requireLogicalTrimTransition(previous, next);
            if (next.lastActivationOrdinal() < previous.lastActivationOrdinal()
                    || next.retiredHistory().count() < previous.retiredHistory().count()
                    || (next.retiredHistory().count()
                                    == previous.retiredHistory().count()
                            && !next.retiredHistory().equals(previous.retiredHistory()))) {
                throw new IllegalArgumentException(
                        "lifecycle selector history cannot roll back or replace a current root");
            }
        }
        if (!wasAuthority && next.bootstrap().isPresent()) {
            throw new IllegalArgumentException("legacy selectors cannot import first-admission provenance");
        }
        if (!wasAuthority
                && (next.taskSelectionDecision().isPresent()
                        || next.materializationWorker().isPresent())) {
            throw new IllegalArgumentException("task selection requires explicit M5 authority migration first");
        }
        if (next.authorityGeneration() != expectedGeneration
                || !next.predecessorValueSha256().equals(Optional.of(before.canonicalStoredSha256()))) {
            throw new IllegalArgumentException("lifecycle selector must name its exact next revision and predecessor");
        }
    }

    private CompletionStage<Void> requireTaskDecisionAuthority(
            Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
        if (key.startsWith(decisionPrefix)) {
            var decision = M5TaskSelectionDecisionV2.decode(candidate);
            return read(key).thenCompose(existing -> {
                if (existing.isPresent()) {
                    if (!existing.orElseThrow().canonicalStoredBytes().equals(candidate)) {
                        return CompletableFuture.failedFuture(
                                new IllegalArgumentException("task decision archive conflicts"));
                    }
                    return CompletableFuture.completedFuture(null);
                }
                return read(keys.selector()).thenApply(selected -> {
                    var anchor = selected.filter(
                                    value -> M5BindingAuthorityCodecV1.isAuthorityValue(value.canonicalStoredBytes()))
                            .flatMap(value -> M5BindingAuthorityCodecV1.decodeAuthority(value.canonicalStoredBytes())
                                    .taskSelectionDecision());
                    if (!anchor.equals(Optional.of(decision))) {
                        throw new IllegalArgumentException(
                                "task decision archive lacks its exact durable selector anchor");
                    }
                    return null;
                });
            });
        }
        if (!key.equals(keys.selector())
                || predecessor.isEmpty()
                || !M5BindingAuthorityCodecV1.isAuthorityValue(candidate)) {
            return CompletableFuture.completedFuture(null);
        }
        var prior = predecessor.orElseThrow().canonicalStoredBytes();
        if (M5BindingBootstrapV1.isBootstrap(prior)) {
            return CompletableFuture.completedFuture(null);
        }
        var previous = M5BindingAuthorityCodecV1.isAuthorityValue(prior)
                ? M5BindingAuthorityCodecV1.decodeAuthority(prior)
                : M5BindingAuthorityCodecV1.migrateLegacy(prior);
        var next = M5BindingAuthorityCodecV1.decodeAuthority(candidate);
        if (previous.taskSelectionDecision().equals(next.taskSelectionDecision())) {
            return CompletableFuture.completedFuture(null);
        }
        if (next.taskSelectionDecision().isPresent()) {
            var decision = next.taskSelectionDecision().orElseThrow();
            return read(decision.key()).thenApply(existing -> {
                if (existing.isPresent()) {
                    throw new IllegalArgumentException(
                            "a task with an archived selection decision cannot select again");
                }
                return null;
            });
        }
        var decision = previous.taskSelectionDecision().orElseThrow();
        return read(decision.key()).thenApply(archive -> {
            if (!archive.map(VersionedValue::canonicalStoredBytes).equals(Optional.of(decision.encode()))) {
                throw new IllegalArgumentException("inline task decision cannot be cleared without its exact archive");
            }
            return null;
        });
    }

    private final class RoutedClient implements OxiaConditionalClient {
        private final OxiaConditionalClient nativeClient;

        private RoutedClient(OxiaConditionalClient nativeClient) {
            this.nativeClient = nativeClient;
        }

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            String qualified = nativeKey(key);
            return nativeClient
                    .read(qualified)
                    .thenApply(observed -> observed.map(value -> {
                        if (!qualified.equals(value.key())) {
                            throw new IllegalStateException("native lifecycle read returned another key");
                        }
                        verifyValue(key, value.storedBytes());
                        return new AuthorityRecord(key, value.storedBytes(), value.versionId());
                    }));
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
            return nativeClient.createIfAbsent(nativeKey(key), value);
        }

        public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
            return nativeClient.compareAndSet(nativeKey(key), value, version);
        }
    }

    private final class CanonicalView implements CanonicalControlMetadataStore {
        public Optional<CanonicalBytes> get(String key) {
            return read(key).toCompletableFuture().join().map(VersionedValue::canonicalStoredBytes);
        }

        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
            return compareAndSet(key, Optional.empty(), value);
        }

        public ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
            nativeKey(key);
            verifyValue(key, value);
            Objects.requireNonNull(expected, "expected");
            return read(key)
                    .thenCompose(before -> {
                        if (!before.map(VersionedValue::canonicalStoredBytes).equals(expected)) {
                            return CompletableFuture.completedFuture(
                                    before.map(VersionedValue::canonicalStoredBytes)
                                                    .equals(Optional.of(value))
                                            ? ControlMutationOutcome.APPLIED
                                            : ControlMutationOutcome.DEFINITIVE_CONFLICT);
                        }
                        return OxiaBindingLifecycleMetadataStoreV2.this
                                .compareAndSet(before, key, value)
                                .thenApply(outcome -> switch (outcome) {
                                    case APPLIED_EXACT -> ControlMutationOutcome.APPLIED;
                                    case PREDECESSOR_UNCHANGED, DEFINITIVE_CONFLICT ->
                                        ControlMutationOutcome.DEFINITIVE_CONFLICT;
                                    case RESPONSE_UNKNOWN -> ControlMutationOutcome.RESPONSE_UNKNOWN;
                                });
                    })
                    .exceptionally(failure -> ControlMutationOutcome.RESPONSE_UNKNOWN)
                    .toCompletableFuture()
                    .join();
        }
    }
}
