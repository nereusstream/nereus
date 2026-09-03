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

package com.nereusstream.storage.object.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorSnapshotRequestV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceProofRequestV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceScanSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class M5RetentionEvidenceAssemblerV1Test {
    private static final CapabilityBinding CAPABILITY = new CapabilityBinding(7, digest("capability"));
    private static final BindingIdentity BINDING =
            new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage-epoch"));
    private static final IdentityEnvelope IDENTITY =
            new IdentityEnvelope(digest("cell"), digest("provider-scope"), BINDING, 11, 13, 17, CAPABILITY);
    private static final ProtocolCoverage COVERAGE = new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, 0, 1_000);

    @Test
    void closedRegistriesBuildFreshCanonicalSnapshotAndProof() {
        InMemoryStore metadata = new InMemoryStore();
        AuthorityFactV1 owner = fact(metadata.seed("/owner", bytes("owner")));
        AuthorityFactV1 storage = fact(metadata.seed("/storage", bytes("storage")));
        M5RetentionEvidenceAssemblerV1 assembler = new M5RetentionEvidenceAssemblerV1(
                metadata,
                floorAdapters(metadata, Optional.empty()),
                referenceAdapters(metadata, Set.of(), Optional.empty()));

        RetentionFloorSnapshotV1 snapshot = assembler
                .buildFloorSnapshot(new FloorSnapshotRequestV1(
                        IDENTITY, PositionDomain.KAFKA_OFFSET, 1, 0, digest("policy"), owner, storage))
                .toCompletableFuture()
                .join();

        assertThat(snapshot.rows()).hasSize(FloorClassV1.values().length);
        assertThat(snapshot.pageCount()).isEqualTo(FloorClassV1.values().length);
        assertThat(snapshot.minimumSafeFloor()).isEqualTo(100);
        assertThat(metadata.lastReadKeys(2)).containsExactly("/owner", "/storage");
        assertThat(M5RetentionCodecV1.decodeSnapshot(M5RetentionCodecV1.encodeSnapshot(snapshot)))
                .isEqualTo(snapshot);

        AuthorityFactV1 selector = fact(metadata.seed("/selector", bytes("selector")));
        AuthorityFactV1 manifest = fact(metadata.seed("/manifest", bytes("manifest")));
        AuthorityFactV1 trim = fact(metadata.seed("/trim", bytes("trim")));
        AuthorityFactV1 worker = fact(metadata.seed("/worker", bytes("worker")));
        AuthorityFactV1 provider = fact(metadata.seed("/provider", bytes("provider")));
        ReferenceFreeProofV1 proof = assembler
                .buildReferenceFreeProof(new ReferenceProofRequestV1(
                        IDENTITY,
                        ReferenceTargetKindV1.PULSAR_AGGREGATE,
                        digest("target"),
                        COVERAGE,
                        selector,
                        manifest,
                        trim,
                        snapshot.snapshotRootSha256(),
                        List.of(),
                        owner,
                        worker,
                        storage,
                        provider,
                        1_000,
                        1_001))
                .toCompletableFuture()
                .join();

        assertThat(proof.observations()).hasSize(ReferenceKindV1.values().length);
        assertThat(proof.scanSummaries()).hasSize(ReferenceKindV1.values().length);
        assertThat(metadata.lastReadKeys(4)).containsExactly("/owner", "/worker", "/storage", "/provider");
        CanonicalBytes encodedProof = M5RetentionCodecV1.encodeReferenceFreeProof(proof);
        assertThat(M5RetentionCodecV1.decodeReferenceFreeProof(encodedProof)).isEqualTo(proof);
        byte[] unknownKind = encodedProof.toByteArray();
        byte[] firstSummary = ByteBuffer.allocate(18)
                .put((byte) 1)
                .putInt(1)
                .putInt(1)
                .putLong(64)
                .put((byte) 1)
                .array();
        unknownKind[findUnique(unknownKind, firstSummary)] = (byte) 0xff;
        assertThatThrownBy(() -> M5RetentionCodecV1.decodeReferenceFreeProof(CanonicalBytes.copyOf(unknownKind)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reference kind");
    }

    @ParameterizedTest
    @EnumSource(FloorClassV1.class)
    void everyMissingFloorAdapterIsRejectedBeforeAnyScan(FloorClassV1 missing) {
        InMemoryStore metadata = new InMemoryStore();
        EnumMap<FloorClassV1, FloorAdapterV1> incomplete = floorAdapters(metadata, Optional.empty());
        incomplete.remove(missing);

        assertThatThrownBy(() -> new M5RetentionEvidenceAssemblerV1(
                        metadata, incomplete, referenceAdapters(metadata, Set.of(), Optional.empty())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("closed inventory");
        assertThat(metadata.readCalls).isZero();
    }

    @ParameterizedTest
    @EnumSource(ReferenceKindV1.class)
    void everyMissingReferenceAdapterIsRejectedBeforeAnyScan(ReferenceKindV1 missing) {
        InMemoryStore metadata = new InMemoryStore();
        EnumMap<ReferenceKindV1, ReferenceAdapterV1> incomplete =
                referenceAdapters(metadata, Set.of(), Optional.empty());
        incomplete.remove(missing);

        assertThatThrownBy(() -> new M5RetentionEvidenceAssemblerV1(
                        metadata, floorAdapters(metadata, Optional.empty()), incomplete))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("closed inventory");
        assertThat(metadata.readCalls).isZero();
    }

    @ParameterizedTest
    @EnumSource(FloorClassV1.class)
    void everyFloorAuthorityVersionChangeDuringScanIsRejected(FloorClassV1 changed) {
        InMemoryStore metadata = new InMemoryStore();
        AuthorityFactV1 owner = fact(metadata.seed("/owner/change", bytes("owner")));
        AuthorityFactV1 storage = fact(metadata.seed("/storage/change", bytes("storage")));
        M5RetentionEvidenceAssemblerV1 assembler = new M5RetentionEvidenceAssemblerV1(
                metadata,
                floorAdapters(metadata, Optional.of(changed)),
                referenceAdapters(metadata, Set.of(), Optional.empty()));

        assertThatThrownBy(() -> assembler
                        .buildFloorSnapshot(new FloorSnapshotRequestV1(
                                IDENTITY, PositionDomain.KAFKA_OFFSET, 1, 0, digest("policy"), owner, storage))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(M5ReferenceFreshnessVerifierV1.StaleAuthorityException.class);
    }

    @ParameterizedTest
    @EnumSource(ReferenceKindV1.class)
    void everyPresentReferenceKindVetoesProofAssembly(ReferenceKindV1 present) {
        InMemoryStore metadata = new InMemoryStore();
        M5RetentionEvidenceAssemblerV1 assembler = new M5RetentionEvidenceAssemblerV1(
                metadata,
                floorAdapters(metadata, Optional.empty()),
                referenceAdapters(metadata, EnumSet.of(present), Optional.empty()));

        assertThatThrownBy(() -> assembler
                        .buildReferenceFreeProof(referenceRequest(metadata, "present-" + present.ordinal()))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("present");
    }

    @ParameterizedTest
    @EnumSource(ReferenceKindV1.class)
    void everyReferenceAuthorityVersionChangeDuringScanIsRejected(ReferenceKindV1 changed) {
        InMemoryStore metadata = new InMemoryStore();
        M5RetentionEvidenceAssemblerV1 assembler = new M5RetentionEvidenceAssemblerV1(
                metadata,
                floorAdapters(metadata, Optional.empty()),
                referenceAdapters(metadata, Set.of(), Optional.of(changed)));

        assertThatThrownBy(() -> assembler
                        .buildReferenceFreeProof(referenceRequest(metadata, "changed-" + changed.ordinal()))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(M5ReferenceFreshnessVerifierV1.StaleAuthorityException.class);
    }

    @Test
    void multiplePresentReferenceKindsRemainACombinedVeto() {
        InMemoryStore metadata = new InMemoryStore();
        M5RetentionEvidenceAssemblerV1 assembler = new M5RetentionEvidenceAssemblerV1(
                metadata,
                floorAdapters(metadata, Optional.empty()),
                referenceAdapters(
                        metadata,
                        EnumSet.of(ReferenceKindV1.MANIFEST_SELECTED, ReferenceKindV1.AUDIT_GRACE),
                        Optional.empty()));

        assertThatThrownBy(() -> assembler
                        .buildReferenceFreeProof(referenceRequest(metadata, "combined"))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("present");
    }

    private static EnumMap<FloorClassV1, FloorAdapterV1> floorAdapters(
            InMemoryStore metadata, Optional<FloorClassV1> changedKind) {
        EnumMap<FloorClassV1, FloorAdapterV1> adapters = new EnumMap<>(FloorClassV1.class);
        for (FloorClassV1 floorClass : FloorClassV1.values()) {
            adapters.put(floorClass, request -> {
                String key = "/floor/" + floorClass.ordinal() + "/" + metadata.nextFixtureOrdinal();
                VersionedValue value = metadata.seed(key, bytes(floorClass.name()));
                if (changedKind.filter(kind -> kind == floorClass).isPresent()) {
                    metadata.overwrite(key, bytes("changed"));
                }
                RetentionFloorObservationV1 row = new RetentionFloorObservationV1(
                        floorClass, fact(value), request.domain(), 100 + floorClass.ordinal(), true, true);
                return CompletableFuture.completedFuture(new FloorAdapterResultV1(floorClass, 1, 64, List.of(row)));
            });
        }
        return adapters;
    }

    private static EnumMap<ReferenceKindV1, ReferenceAdapterV1> referenceAdapters(
            InMemoryStore metadata, Set<ReferenceKindV1> presentKinds, Optional<ReferenceKindV1> changedKind) {
        EnumMap<ReferenceKindV1, ReferenceAdapterV1> adapters = new EnumMap<>(ReferenceKindV1.class);
        for (ReferenceKindV1 referenceKind : ReferenceKindV1.values()) {
            adapters.put(referenceKind, request -> {
                String key = "/reference/" + referenceKind.ordinal() + "/" + metadata.nextFixtureOrdinal();
                VersionedValue value = metadata.seed(key, bytes(referenceKind.name()));
                if (changedKind.filter(kind -> kind == referenceKind).isPresent()) {
                    metadata.overwrite(key, bytes("changed"));
                }
                ReferenceObservationV1 row = new ReferenceObservationV1(
                        referenceKind,
                        fact(value),
                        request.targetIdentitySha256(),
                        request.coverage(),
                        presentKinds.contains(referenceKind)
                                ? ReferenceDispositionV1.PRESENT
                                : ReferenceDispositionV1.ABSENT,
                        true);
                return CompletableFuture.completedFuture(new ReferenceAdapterResultV1(
                        referenceKind, new ReferenceScanSummaryV1(referenceKind, 1, 1, 64, true), List.of(row)));
            });
        }
        return adapters;
    }

    private static ReferenceProofRequestV1 referenceRequest(InMemoryStore metadata, String suffix) {
        AuthorityFactV1 owner = fact(metadata.seed("/owner/" + suffix, bytes("owner")));
        AuthorityFactV1 storage = fact(metadata.seed("/storage/" + suffix, bytes("storage")));
        AuthorityFactV1 selector = fact(metadata.seed("/selector/" + suffix, bytes("selector")));
        AuthorityFactV1 manifest = fact(metadata.seed("/manifest/" + suffix, bytes("manifest")));
        AuthorityFactV1 trim = fact(metadata.seed("/trim/" + suffix, bytes("trim")));
        AuthorityFactV1 worker = fact(metadata.seed("/worker/" + suffix, bytes("worker")));
        AuthorityFactV1 provider = fact(metadata.seed("/provider/" + suffix, bytes("provider")));
        return new ReferenceProofRequestV1(
                IDENTITY,
                ReferenceTargetKindV1.PULSAR_AGGREGATE,
                digest("target"),
                COVERAGE,
                selector,
                manifest,
                trim,
                digest("snapshot"),
                List.of(),
                owner,
                worker,
                storage,
                provider,
                1_000,
                1_001);
    }

    private static AuthorityFactV1 fact(VersionedValue value) {
        return new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(bytes(value));
    }

    private static int findUnique(byte[] value, byte[] pattern) {
        int found = -1;
        for (int offset = 0; offset <= value.length - pattern.length; offset++) {
            boolean matches = true;
            for (int index = 0; index < pattern.length; index++) {
                if (value[offset + index] != pattern[index]) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                if (found >= 0) {
                    throw new IllegalArgumentException("test pattern is not unique");
                }
                found = offset;
            }
        }
        if (found < 0) {
            throw new IllegalArgumentException("test pattern is absent");
        }
        return found;
    }

    private static final class InMemoryStore implements ExactMetadataTransactionStoreV1 {
        private final Map<String, VersionedValue> values = new java.util.LinkedHashMap<>();
        private final List<String> readKeys = new java.util.ArrayList<>();
        private long version;
        private long fixtureOrdinal;
        private int readCalls;

        synchronized long nextFixtureOrdinal() {
            return fixtureOrdinal++;
        }

        synchronized List<String> lastReadKeys(int count) {
            return List.copyOf(readKeys.subList(readKeys.size() - count, readKeys.size()));
        }

        synchronized VersionedValue seed(String key, CanonicalBytes value) {
            if (values.containsKey(key)) {
                throw new IllegalArgumentException("duplicate test seed: " + key);
            }
            VersionedValue stored = stored(key, value);
            values.put(key, stored);
            return stored;
        }

        synchronized VersionedValue overwrite(String key, CanonicalBytes value) {
            if (!values.containsKey(key)) {
                throw new IllegalArgumentException("missing test value: " + key);
            }
            VersionedValue stored = stored(key, value);
            values.put(key, stored);
            return stored;
        }

        @Override
        public synchronized CompletionStage<Optional<VersionedValue>> read(String key) {
            readCalls++;
            readKeys.add(key);
            return CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        @Override
        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> exactPredecessor, String key, CanonicalBytes exactCandidate) {
            return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
        }

        @Override
        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
        }

        @Override
        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }

        private VersionedValue stored(String key, CanonicalBytes value) {
            MetadataVersion metadataVersion = new MetadataVersion(CanonicalBytes.copyOf(
                    ByteBuffer.allocate(Long.BYTES).putLong(version++).array()));
            return VersionedValue.of(key, value, metadataVersion);
        }
    }
}
