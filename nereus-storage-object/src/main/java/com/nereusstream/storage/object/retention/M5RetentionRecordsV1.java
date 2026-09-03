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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/** Closed immutable M5-C logical-retention, reference-proof, and tombstone values. */
public final class M5RetentionRecordsV1 {
    public static final int MAX_FLOOR_ROWS = 4_096;
    public static final int MAX_REFERENCE_ROWS = 16_384;
    public static final int MAX_SCAN_PAGES = 4_096;
    public static final long MAX_SCAN_BYTES = 256L * 1024 * 1024;
    public static final int MAX_RELEASE_BINDINGS = 64;
    public static final int MAX_FULL_BATCH_BYTES = M4ReadControlCodecV1.MAX_CONTROL_BYTES;

    private M5RetentionRecordsV1() {}

    /** Closed authoritative floor inventory from the accepted M5-C design. */
    public enum FloorClassV1 {
        BINDING_EPOCH_POLICY,
        KAFKA_CONSUMER_GROUP,
        KAFKA_PRODUCER_TRANSACTION,
        KAFKA_REPLICATION_RECOVERY,
        PULSAR_CURSOR_SUBSCRIPTION,
        GENERATION_READ,
        LIFECYCLE_TASK,
        SHARED_PHYSICAL_SOURCE,
        PROJECTION_MIGRATION,
        AUDIT_GRACE
    }

    /** Closed reference-veto inventory; enum order is a persistent wire property. */
    public enum ReferenceKindV1 {
        MANIFEST_SELECTED,
        MANIFEST_FALLBACK,
        READ_GENERATION_PIN_OR_OPEN_HANDLE,
        SOURCE_PROTECTION,
        KAFKA_GROUP_RETENTION,
        KAFKA_PRODUCER_RECOVERY,
        KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
        KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY,
        PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR,
        RECOVERY_CHECKPOINT_OR_SNAPSHOT,
        MATERIALIZATION_OR_COMPACTION_TASK,
        RETIREMENT_OR_DELETE_RECONCILIATION,
        SHARED_PHYSICAL_MEMBER,
        PROJECTION_MIGRATION_OR_EXPORT,
        AUDIT_GRACE
    }

    public enum ReferenceDispositionV1 {
        PRESENT,
        ABSENT
    }

    public enum ReferenceTargetKindV1 {
        READABLE_SOURCE,
        RETIREMENT_BATCH,
        PULSAR_AGGREGATE,
        UNSELECTED_OUTPUT
    }

    public enum BatchMetadataStateV1 {
        FULL_V1,
        RETIRED_V1
    }

    /** Version/value commitment used by a version-vector freshness check. */
    public record AuthorityFactV1(String key, MetadataVersion metadataVersion, Sha256Digest valueSha256) {
        public AuthorityFactV1 {
            requireText(key, "key");
            Objects.requireNonNull(metadataVersion, "metadataVersion");
            if (metadataVersion.value().isEmpty()) {
                throw new IllegalArgumentException("authority metadata version is empty");
            }
            requireDigest(valueSha256, "valueSha256");
        }
    }

    /** One authoritative class floor, or one explicit accepted non-vetoing policy fact. */
    public record RetentionFloorObservationV1(
            FloorClassV1 floorClass,
            AuthorityFactV1 authority,
            PositionDomain domain,
            long safeFloor,
            boolean constraining,
            boolean enumerationComplete) {
        public RetentionFloorObservationV1 {
            Objects.requireNonNull(floorClass, "floorClass");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(domain, "domain");
            if (!enumerationComplete) {
                throw new IllegalArgumentException("partial floor enumeration cannot enter a retention snapshot");
            }
            if ((constraining && safeFloor < 0) || (!constraining && safeFloor != -1)) {
                throw new IllegalArgumentException("floor value and constraining policy disagree");
            }
        }
    }

    /** Complete closed floor snapshot, rooted independently from its identity field. */
    public record RetentionFloorSnapshotV1(
            IdentityEnvelope identity,
            PositionDomain domain,
            long generation,
            long priorTrimFrontier,
            Sha256Digest retentionPolicyRootSha256,
            AuthorityFactV1 ownerFence,
            AuthorityFactV1 storageFence,
            int pageCount,
            long scannedBytes,
            List<RetentionFloorObservationV1> rows,
            Sha256Digest snapshotRootSha256) {
        public RetentionFloorSnapshotV1 {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(domain, "domain");
            requireDigest(retentionPolicyRootSha256, "retentionPolicyRootSha256");
            Objects.requireNonNull(ownerFence, "ownerFence");
            Objects.requireNonNull(storageFence, "storageFence");
            rows = sortedFloorRows(rows);
            requireDigest(snapshotRootSha256, "snapshotRootSha256");
            if (generation <= 0
                    || priorTrimFrontier < 0
                    || pageCount <= 0
                    || pageCount > MAX_SCAN_PAGES
                    || scannedBytes <= 0
                    || scannedBytes > MAX_SCAN_BYTES) {
                throw new IllegalArgumentException("retention snapshot generation/scan bounds are invalid");
            }
            if (rows.stream().anyMatch(row -> row.domain() != domain)) {
                throw new IllegalArgumentException("retention floor Position Domain differs");
            }
            EnumSet<FloorClassV1> present = EnumSet.noneOf(FloorClassV1.class);
            rows.forEach(row -> present.add(row.floorClass()));
            if (!present.equals(EnumSet.allOf(FloorClassV1.class))) {
                throw new IllegalArgumentException("retention snapshot omits a closed floor class");
            }
            if (rows.stream().noneMatch(RetentionFloorObservationV1::constraining)) {
                throw new IllegalArgumentException("retention snapshot has no constraining floor");
            }
        }

        public long minimumSafeFloor() {
            return rows.stream()
                    .filter(RetentionFloorObservationV1::constraining)
                    .mapToLong(RetentionFloorObservationV1::safeFloor)
                    .min()
                    .orElseThrow();
        }
    }

    /** Monotonic typed logical frontier; this value grants no release or delete authority. */
    public record BindingTrimFrontierV1(
            IdentityEnvelope identity,
            PositionDomain domain,
            long priorFrontier,
            long newFrontier,
            Sha256Digest retentionPolicyRootSha256,
            Sha256Digest floorSnapshotRootSha256,
            AuthorityFactV1 ownerFence,
            AuthorityFactV1 storageFence,
            long generation,
            CapabilityBinding capability) {
        public BindingTrimFrontierV1 {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(domain, "domain");
            requireDigest(retentionPolicyRootSha256, "retentionPolicyRootSha256");
            requireDigest(floorSnapshotRootSha256, "floorSnapshotRootSha256");
            Objects.requireNonNull(ownerFence, "ownerFence");
            Objects.requireNonNull(storageFence, "storageFence");
            Objects.requireNonNull(capability, "capability");
            if (priorFrontier < 0 || newFrontier < priorFrontier || generation <= 0) {
                throw new IllegalArgumentException("logical trim frontier regresses or has an invalid generation");
            }
            if (!capability.equals(identity.capability())) {
                throw new IllegalArgumentException("trim capability differs from its identity envelope");
            }
        }
    }

    /** One exact reference observation; absence requires a complete authoritative enumeration. */
    public record ReferenceObservationV1(
            ReferenceKindV1 kind,
            AuthorityFactV1 authority,
            Sha256Digest targetIdentitySha256,
            ProtocolCoverage coverage,
            ReferenceDispositionV1 disposition,
            boolean enumerationComplete) {
        public ReferenceObservationV1 {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(authority, "authority");
            requireDigest(targetIdentitySha256, "targetIdentitySha256");
            Objects.requireNonNull(coverage, "coverage");
            Objects.requireNonNull(disposition, "disposition");
            if (disposition == ReferenceDispositionV1.ABSENT && !enumerationComplete) {
                throw new IllegalArgumentException("reference absence requires complete authoritative enumeration");
            }
        }
    }

    /** Per-kind bounded scan summary. Every kind must have exactly one summary. */
    public record ReferenceScanSummaryV1(
            ReferenceKindV1 kind, int rowCount, int pageCount, long scannedBytes, boolean complete) {
        public ReferenceScanSummaryV1 {
            Objects.requireNonNull(kind, "kind");
            if (rowCount <= 0
                    || rowCount > MAX_REFERENCE_ROWS
                    || pageCount <= 0
                    || pageCount > MAX_SCAN_PAGES
                    || scannedBytes <= 0
                    || scannedBytes > MAX_SCAN_BYTES
                    || !complete) {
                throw new IllegalArgumentException("reference scan summary is partial or outside its hard caps");
            }
        }
    }

    /** Exact M4 RELEASED witness for one source/protection generation. */
    public record M4ReleaseBindingV1(
            Sha256Digest sourceIdentitySha256,
            long protectionGeneration,
            AuthorityFactV1 protectionAuthority,
            CanonicalBytes canonicalProtectionBytes,
            Sha256Digest releasedByBatchSha256,
            Sha256Digest releaseProofHeadSha256) {
        public M4ReleaseBindingV1 {
            requireDigest(sourceIdentitySha256, "sourceIdentitySha256");
            Objects.requireNonNull(protectionAuthority, "protectionAuthority");
            Objects.requireNonNull(canonicalProtectionBytes, "canonicalProtectionBytes");
            requireDigest(releasedByBatchSha256, "releasedByBatchSha256");
            requireDigest(releaseProofHeadSha256, "releaseProofHeadSha256");
            if (protectionGeneration <= 0) {
                throw new IllegalArgumentException("M4 protection generation must be positive");
            }
            if (!Sha256Digest.hash(canonicalProtectionBytes).equals(protectionAuthority.valueSha256())) {
                throw new IllegalArgumentException("M4 protection bytes differ from their authority SHA-256");
            }
            SourceProtection protection = M4ReadControlCodecV1.decodeProtection(canonicalProtectionBytes);
            if (!protection.identity().sourceIdentitySha256().equals(sourceIdentitySha256)
                    || protection.identity().protectionGeneration() != protectionGeneration
                    || protection.state() != ProtectionState.RELEASED
                    || !protection.releasedByBatchSha256().equals(java.util.Optional.of(releasedByBatchSha256))
                    || !protection.releaseProofHeadSha256().equals(java.util.Optional.of(releaseProofHeadSha256))) {
                throw new IllegalArgumentException("M4 protection is not the exact bound RELEASED value");
            }
        }
    }

    /** Immutable proof accepted only when every closed reference class is authoritatively absent. */
    public record ReferenceFreeProofV1(
            IdentityEnvelope identity,
            ReferenceTargetKindV1 targetKind,
            Sha256Digest targetIdentitySha256,
            ProtocolCoverage coverage,
            AuthorityFactV1 selectorRoot,
            AuthorityFactV1 manifestRoot,
            AuthorityFactV1 trimRoot,
            Sha256Digest retentionSnapshotRootSha256,
            Sha256Digest observationsRootSha256,
            List<M4ReleaseBindingV1> m4Releases,
            AuthorityFactV1 ownerFence,
            AuthorityFactV1 workerFence,
            AuthorityFactV1 storageFence,
            AuthorityFactV1 providerFence,
            long auditGraceDeadlineMillis,
            long observedAuthorityTimeMillis,
            List<ReferenceScanSummaryV1> scanSummaries,
            List<ReferenceObservationV1> observations,
            Sha256Digest proofSha256) {
        public ReferenceFreeProofV1 {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(targetKind, "targetKind");
            requireDigest(targetIdentitySha256, "targetIdentitySha256");
            Objects.requireNonNull(coverage, "coverage");
            Objects.requireNonNull(selectorRoot, "selectorRoot");
            Objects.requireNonNull(manifestRoot, "manifestRoot");
            Objects.requireNonNull(trimRoot, "trimRoot");
            requireDigest(retentionSnapshotRootSha256, "retentionSnapshotRootSha256");
            requireDigest(observationsRootSha256, "observationsRootSha256");
            m4Releases = sortedReleases(m4Releases);
            Objects.requireNonNull(ownerFence, "ownerFence");
            Objects.requireNonNull(workerFence, "workerFence");
            Objects.requireNonNull(storageFence, "storageFence");
            Objects.requireNonNull(providerFence, "providerFence");
            scanSummaries = sortedScanSummaries(scanSummaries);
            observations = sortedReferenceRows(observations);
            requireDigest(proofSha256, "proofSha256");
            if (auditGraceDeadlineMillis <= 0 || observedAuthorityTimeMillis < auditGraceDeadlineMillis) {
                throw new IllegalArgumentException("reference-free proof audit grace is not satisfied");
            }
            if (observations.stream()
                    .anyMatch(row -> !row.targetIdentitySha256().equals(targetIdentitySha256)
                            || !row.coverage().equals(coverage)
                            || row.disposition() != ReferenceDispositionV1.ABSENT
                            || !row.enumerationComplete())) {
                throw new IllegalArgumentException("reference-free proof contains a present, partial, or foreign row");
            }
            if ((targetKind == ReferenceTargetKindV1.READABLE_SOURCE
                            || targetKind == ReferenceTargetKindV1.RETIREMENT_BATCH)
                    && m4Releases.isEmpty()) {
                throw new IllegalArgumentException("readable-source retirement lacks exact M4 RELEASED bindings");
            }
            requireCompleteReferenceKinds(scanSummaries, observations);
        }
    }

    /** Externalized canonical M4 batch. It is metadata only and grants no deletion authority. */
    public record FullSourceRetirementBatchV1(
            BatchMetadataStateV1 state,
            BindingIdentity binding,
            Sha256Digest batchIdSha256,
            Sha256Digest fullBatchSha256,
            CanonicalBytes canonicalM4BatchBytes,
            Sha256Digest selectorPredecessorValueSha256,
            Sha256Digest referenceFreeProofSha256,
            CapabilityBinding capability) {
        public FullSourceRetirementBatchV1 {
            if (state != BatchMetadataStateV1.FULL_V1) {
                throw new IllegalArgumentException("full batch record must have FULL_V1 state");
            }
            Objects.requireNonNull(binding, "binding");
            requireDigest(batchIdSha256, "batchIdSha256");
            requireDigest(fullBatchSha256, "fullBatchSha256");
            Objects.requireNonNull(canonicalM4BatchBytes, "canonicalM4BatchBytes");
            requireDigest(selectorPredecessorValueSha256, "selectorPredecessorValueSha256");
            requireDigest(referenceFreeProofSha256, "referenceFreeProofSha256");
            Objects.requireNonNull(capability, "capability");
            if (canonicalM4BatchBytes.isEmpty() || canonicalM4BatchBytes.length() > MAX_FULL_BATCH_BYTES) {
                throw new IllegalArgumentException("canonical M4 batch bytes exceed their hard cap");
            }
            SourceRetirementBatch decoded = M4ReadControlCodecV1.decodeBatch(canonicalM4BatchBytes);
            if (!decoded.binding().equals(binding)
                    || !decoded.batchIdSha256().equals(batchIdSha256)
                    || !Sha256Digest.hash(canonicalM4BatchBytes).equals(fullBatchSha256)
                    || !decoded.capability().equals(capability)) {
                throw new IllegalArgumentException("FULL_V1 record differs from its canonical M4 batch");
            }
        }
    }

    /** Permanent same-key compact Object-WAL batch tombstone. */
    public record RetiredSourceRetirementBatchTombstoneV1(
            BatchMetadataStateV1 state,
            BindingIdentity binding,
            Sha256Digest batchIdSha256,
            Sha256Digest fullBatchSha256,
            Sha256Digest referenceFreeProofSha256,
            MetadataVersion fullPredecessorVersion,
            Sha256Digest fullPredecessorValueSha256,
            CapabilityBinding capability,
            Sha256Digest tombstoneCanonicalSha256) {
        public RetiredSourceRetirementBatchTombstoneV1 {
            if (state != BatchMetadataStateV1.RETIRED_V1) {
                throw new IllegalArgumentException("retired batch tombstone must have RETIRED_V1 state");
            }
            Objects.requireNonNull(binding, "binding");
            requireDigest(batchIdSha256, "batchIdSha256");
            requireDigest(fullBatchSha256, "fullBatchSha256");
            requireDigest(referenceFreeProofSha256, "referenceFreeProofSha256");
            Objects.requireNonNull(fullPredecessorVersion, "fullPredecessorVersion");
            requireDigest(fullPredecessorValueSha256, "fullPredecessorValueSha256");
            Objects.requireNonNull(capability, "capability");
            requireDigest(tombstoneCanonicalSha256, "tombstoneCanonicalSha256");
        }
    }

    /** Exact M5-D cleanup-root summary consumed by final Pulsar aggregate retirement. */
    public record PhysicalCleanupSummaryV1(
            AuthorityFactV1 cleanupRoot,
            PulsarTopicIncarnationIdentity incarnation,
            TopicBindingId bindingId,
            Sha256Digest originalAggregateSha256,
            CapabilityBinding capability,
            int requiredSources,
            int deleteDoneSources,
            int authoritativelyAbsentSources,
            int pendingSources) {
        public PhysicalCleanupSummaryV1 {
            Objects.requireNonNull(cleanupRoot, "cleanupRoot");
            Objects.requireNonNull(incarnation, "incarnation");
            Objects.requireNonNull(bindingId, "bindingId");
            requireDigest(originalAggregateSha256, "originalAggregateSha256");
            Objects.requireNonNull(capability, "capability");
            if (requiredSources <= 0
                    || deleteDoneSources < 0
                    || authoritativelyAbsentSources < 0
                    || pendingSources != 0
                    || Math.addExact(deleteDoneSources, authoritativelyAbsentSources) != requiredSources) {
                throw new IllegalArgumentException("physical cleanup summary is incomplete or inconsistent");
            }
        }
    }

    /** Permanent same-key compact Pulsar incarnation tombstone; normally installed after M5-D. */
    public record RetiredTopicIncarnationTombstoneV1(
            PulsarTopicIncarnationIdentity incarnation,
            TopicBindingId bindingId,
            Sha256Digest originalAggregateSha256,
            Sha256Digest referenceFreeProofSha256,
            long selectorGeneration,
            PulsarTopicGenerationSelectorStateV1 selectorState,
            MetadataVersion selectorVersion,
            MetadataVersion aggregatePredecessorVersion,
            Sha256Digest aggregatePredecessorValueSha256,
            CapabilityBinding capability,
            Sha256Digest tombstoneCanonicalSha256) {
        public RetiredTopicIncarnationTombstoneV1 {
            Objects.requireNonNull(incarnation, "incarnation");
            Objects.requireNonNull(bindingId, "bindingId");
            requireDigest(originalAggregateSha256, "originalAggregateSha256");
            requireDigest(referenceFreeProofSha256, "referenceFreeProofSha256");
            if (selectorGeneration <= 0
                    || selectorGeneration != incarnation.bindingGeneration().value()
                    || selectorState != PulsarTopicGenerationSelectorStateV1.DELETED) {
                throw new IllegalArgumentException("Pulsar tombstone lacks exact DELETED incarnation selector");
            }
            Objects.requireNonNull(selectorVersion, "selectorVersion");
            Objects.requireNonNull(aggregatePredecessorVersion, "aggregatePredecessorVersion");
            requireDigest(aggregatePredecessorValueSha256, "aggregatePredecessorValueSha256");
            Objects.requireNonNull(capability, "capability");
            requireDigest(tombstoneCanonicalSha256, "tombstoneCanonicalSha256");
        }
    }

    private static List<RetentionFloorObservationV1> sortedFloorRows(List<RetentionFloorObservationV1> rows) {
        List<RetentionFloorObservationV1> copy = List.copyOf(Objects.requireNonNull(rows, "rows"));
        if (copy.isEmpty() || copy.size() > MAX_FLOOR_ROWS) {
            throw new IllegalArgumentException("retention floor row count is outside its hard cap");
        }
        Comparator<RetentionFloorObservationV1> order = Comparator.comparing(RetentionFloorObservationV1::floorClass)
                .thenComparing(row -> row.authority().key());
        if (!copy.equals(copy.stream().sorted(order).toList())
                || copy.stream()
                                .map(row -> row.floorClass() + "\u0000"
                                        + row.authority().key())
                                .distinct()
                                .count()
                        != copy.size()) {
            throw new IllegalArgumentException("retention floor rows are not sorted unique");
        }
        return copy;
    }

    private static List<M4ReleaseBindingV1> sortedReleases(List<M4ReleaseBindingV1> rows) {
        List<M4ReleaseBindingV1> copy = List.copyOf(Objects.requireNonNull(rows, "m4Releases"));
        if (copy.size() > MAX_RELEASE_BINDINGS) {
            throw new IllegalArgumentException("M4 release binding count exceeds its hard cap");
        }
        Comparator<M4ReleaseBindingV1> order = Comparator.comparing(
                        (M4ReleaseBindingV1 row) -> row.sourceIdentitySha256().toHex())
                .thenComparingLong(M4ReleaseBindingV1::protectionGeneration);
        if (!copy.equals(copy.stream().sorted(order).toList())
                || copy.stream().distinct().count() != copy.size()) {
            throw new IllegalArgumentException("M4 release bindings are not sorted unique");
        }
        return copy;
    }

    private static List<ReferenceScanSummaryV1> sortedScanSummaries(List<ReferenceScanSummaryV1> rows) {
        List<ReferenceScanSummaryV1> copy = List.copyOf(Objects.requireNonNull(rows, "scanSummaries"));
        if (copy.size() != ReferenceKindV1.values().length
                || !copy.equals(copy.stream()
                        .sorted(Comparator.comparing(ReferenceScanSummaryV1::kind))
                        .toList())
                || copy.stream().map(ReferenceScanSummaryV1::kind).distinct().count() != copy.size()) {
            throw new IllegalArgumentException("reference summaries do not cover the closed sorted inventory");
        }
        return copy;
    }

    private static List<ReferenceObservationV1> sortedReferenceRows(List<ReferenceObservationV1> rows) {
        List<ReferenceObservationV1> copy = List.copyOf(Objects.requireNonNull(rows, "observations"));
        if (copy.isEmpty() || copy.size() > MAX_REFERENCE_ROWS) {
            throw new IllegalArgumentException("reference row count is outside its hard cap");
        }
        Comparator<ReferenceObservationV1> order = Comparator.comparing(ReferenceObservationV1::kind)
                .thenComparing(row -> row.authority().key())
                .thenComparingLong(row -> row.coverage().inclusiveStart())
                .thenComparingLong(row -> row.coverage().exclusiveEnd());
        if (!copy.equals(copy.stream().sorted(order).toList())
                || copy.stream().distinct().count() != copy.size()) {
            throw new IllegalArgumentException("reference observations are not sorted unique");
        }
        return copy;
    }

    private static void requireCompleteReferenceKinds(
            List<ReferenceScanSummaryV1> summaries, List<ReferenceObservationV1> observations) {
        EnumSet<ReferenceKindV1> observedKinds = EnumSet.noneOf(ReferenceKindV1.class);
        observations.forEach(row -> observedKinds.add(row.kind()));
        if (!observedKinds.equals(EnumSet.allOf(ReferenceKindV1.class))) {
            throw new IllegalArgumentException("reference-free proof omits a closed reference kind");
        }
        for (ReferenceScanSummaryV1 summary : summaries) {
            long exactRows = observations.stream()
                    .filter(row -> row.kind() == summary.kind())
                    .count();
            if (exactRows != summary.rowCount()) {
                throw new IllegalArgumentException("reference summary row count differs from observations");
            }
        }
    }

    static void requireDigest(Sha256Digest digest, String name) {
        Objects.requireNonNull(digest, name);
        if (digest.isZero()) {
            throw new IllegalArgumentException(name + " is zero");
        }
    }

    static void requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " is blank");
        }
    }
}
