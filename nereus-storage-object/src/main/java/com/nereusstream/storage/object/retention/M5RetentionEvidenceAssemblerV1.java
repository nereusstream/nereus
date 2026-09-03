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
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.M4ReleaseBindingV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceScanSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Closed-registry M5-C floor/reference assembler.
 *
 * <p>Every enum member must have one adapter. Results are accepted only after a final exact reread
 * of every returned authority version/value, so cache-only, skipped, missing, or changed pages fail
 * closed before a snapshot or proof becomes observable.
 */
public final class M5RetentionEvidenceAssemblerV1 {
    @FunctionalInterface
    public interface FloorAdapterV1 {
        CompletionStage<FloorAdapterResultV1> scan(FloorSnapshotRequestV1 request);
    }

    @FunctionalInterface
    public interface ReferenceAdapterV1 {
        CompletionStage<ReferenceAdapterResultV1> scan(ReferenceProofRequestV1 request);
    }

    public record FloorSnapshotRequestV1(
            IdentityEnvelope identity,
            PositionDomain domain,
            long generation,
            long priorTrimFrontier,
            Sha256Digest retentionPolicyRootSha256,
            AuthorityFactV1 ownerFence,
            AuthorityFactV1 storageFence) {
        public FloorSnapshotRequestV1 {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(domain, "domain");
            M5RetentionRecordsV1.requireDigest(retentionPolicyRootSha256, "retentionPolicyRootSha256");
            Objects.requireNonNull(ownerFence, "ownerFence");
            Objects.requireNonNull(storageFence, "storageFence");
            if (generation <= 0 || priorTrimFrontier < 0) {
                throw new IllegalArgumentException("floor snapshot request generation/frontier is invalid");
            }
        }
    }

    public record FloorAdapterResultV1(
            FloorClassV1 floorClass, int pageCount, long scannedBytes, List<RetentionFloorObservationV1> observations) {
        public FloorAdapterResultV1 {
            Objects.requireNonNull(floorClass, "floorClass");
            observations = List.copyOf(Objects.requireNonNull(observations, "observations"));
            if (pageCount <= 0
                    || pageCount > M5RetentionRecordsV1.MAX_SCAN_PAGES
                    || scannedBytes <= 0
                    || scannedBytes > M5RetentionRecordsV1.MAX_SCAN_BYTES
                    || observations.isEmpty()
                    || observations.size() > M5RetentionRecordsV1.MAX_FLOOR_ROWS) {
                throw new IllegalArgumentException("floor adapter result is partial or outside its hard caps");
            }
            if (observations.stream().anyMatch(row -> row.floorClass() != floorClass)) {
                throw new IllegalArgumentException("floor adapter returned a foreign floor class");
            }
            requireSortedUniqueFloorRows(observations);
        }
    }

    public record ReferenceProofRequestV1(
            IdentityEnvelope identity,
            ReferenceTargetKindV1 targetKind,
            Sha256Digest targetIdentitySha256,
            ProtocolCoverage coverage,
            AuthorityFactV1 selectorRoot,
            AuthorityFactV1 manifestRoot,
            AuthorityFactV1 trimRoot,
            Sha256Digest retentionSnapshotRootSha256,
            List<M4ReleaseBindingV1> m4Releases,
            AuthorityFactV1 ownerFence,
            AuthorityFactV1 workerFence,
            AuthorityFactV1 storageFence,
            AuthorityFactV1 providerFence,
            long auditGraceDeadlineMillis,
            long observedAuthorityTimeMillis) {
        public ReferenceProofRequestV1 {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(targetKind, "targetKind");
            M5RetentionRecordsV1.requireDigest(targetIdentitySha256, "targetIdentitySha256");
            Objects.requireNonNull(coverage, "coverage");
            Objects.requireNonNull(selectorRoot, "selectorRoot");
            Objects.requireNonNull(manifestRoot, "manifestRoot");
            Objects.requireNonNull(trimRoot, "trimRoot");
            M5RetentionRecordsV1.requireDigest(retentionSnapshotRootSha256, "retentionSnapshotRootSha256");
            m4Releases = List.copyOf(Objects.requireNonNull(m4Releases, "m4Releases"));
            Objects.requireNonNull(ownerFence, "ownerFence");
            Objects.requireNonNull(workerFence, "workerFence");
            Objects.requireNonNull(storageFence, "storageFence");
            Objects.requireNonNull(providerFence, "providerFence");
            if (auditGraceDeadlineMillis <= 0 || observedAuthorityTimeMillis < auditGraceDeadlineMillis) {
                throw new IllegalArgumentException("reference proof request has not satisfied audit grace");
            }
        }
    }

    public record ReferenceAdapterResultV1(
            ReferenceKindV1 referenceKind, ReferenceScanSummaryV1 summary, List<ReferenceObservationV1> observations) {
        public ReferenceAdapterResultV1 {
            Objects.requireNonNull(referenceKind, "referenceKind");
            Objects.requireNonNull(summary, "summary");
            observations = List.copyOf(Objects.requireNonNull(observations, "observations"));
            if (summary.kind() != referenceKind || summary.rowCount() != observations.size()) {
                throw new IllegalArgumentException("reference adapter summary differs from its exact rows");
            }
            if (observations.stream().anyMatch(row -> row.kind() != referenceKind)) {
                throw new IllegalArgumentException("reference adapter returned a foreign reference kind");
            }
            requireSortedUniqueReferenceRows(observations);
        }
    }

    private static final Sha256Digest PLACEHOLDER = Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1}));

    private final Map<FloorClassV1, FloorAdapterV1> floorAdapters;
    private final Map<ReferenceKindV1, ReferenceAdapterV1> referenceAdapters;
    private final M5ReferenceFreshnessVerifierV1 freshness;

    public M5RetentionEvidenceAssemblerV1(
            ExactMetadataTransactionStoreV1 metadata,
            Map<FloorClassV1, FloorAdapterV1> floorAdapters,
            Map<ReferenceKindV1, ReferenceAdapterV1> referenceAdapters) {
        this.freshness = new M5ReferenceFreshnessVerifierV1(Objects.requireNonNull(metadata, "metadata"));
        this.floorAdapters = exactFloorRegistry(floorAdapters);
        this.referenceAdapters = exactReferenceRegistry(referenceAdapters);
    }

    public CompletionStage<RetentionFloorSnapshotV1> buildFloorSnapshot(FloorSnapshotRequestV1 request) {
        Objects.requireNonNull(request, "request");
        CompletionStage<List<FloorAdapterResultV1>> stage = CompletableFuture.completedFuture(new ArrayList<>());
        for (FloorClassV1 floorClass : FloorClassV1.values()) {
            stage = stage.thenCompose(results -> requireStage(
                            floorAdapters.get(floorClass).scan(request), "floor adapter stage")
                    .thenApply(result -> {
                        FloorAdapterResultV1 exact = Objects.requireNonNull(result, "floor adapter result");
                        if (exact.floorClass() != floorClass) {
                            throw new IllegalArgumentException("floor registry key differs from adapter result");
                        }
                        results.add(exact);
                        return results;
                    }));
        }
        return stage.thenCompose(results -> {
            List<RetentionFloorObservationV1> rows = results.stream()
                    .flatMap(result -> result.observations().stream())
                    .sorted(Comparator.comparing(RetentionFloorObservationV1::floorClass)
                            .thenComparing(row -> row.authority().key()))
                    .toList();
            int pageCount =
                    results.stream().mapToInt(FloorAdapterResultV1::pageCount).reduce(0, Math::addExact);
            long scannedBytes = results.stream()
                    .mapToLong(FloorAdapterResultV1::scannedBytes)
                    .reduce(0L, Math::addExact);
            RetentionFloorSnapshotV1 snapshot = M5RetentionCodecV1.finalizeSnapshot(new RetentionFloorSnapshotV1(
                    request.identity(),
                    request.domain(),
                    request.generation(),
                    request.priorTrimFrontier(),
                    request.retentionPolicyRootSha256(),
                    request.ownerFence(),
                    request.storageFence(),
                    pageCount,
                    scannedBytes,
                    rows,
                    PLACEHOLDER));
            return freshness.requireFresh(snapshot).thenApply(ignored -> snapshot);
        });
    }

    public CompletionStage<ReferenceFreeProofV1> buildReferenceFreeProof(ReferenceProofRequestV1 request) {
        Objects.requireNonNull(request, "request");
        CompletionStage<List<ReferenceAdapterResultV1>> stage = CompletableFuture.completedFuture(new ArrayList<>());
        for (ReferenceKindV1 referenceKind : ReferenceKindV1.values()) {
            stage = stage.thenCompose(results -> requireStage(
                            referenceAdapters.get(referenceKind).scan(request), "reference adapter stage")
                    .thenApply(result -> {
                        ReferenceAdapterResultV1 exact = Objects.requireNonNull(result, "reference adapter result");
                        if (exact.referenceKind() != referenceKind) {
                            throw new IllegalArgumentException("reference registry key differs from adapter result");
                        }
                        validateReferenceTarget(request, exact);
                        results.add(exact);
                        return results;
                    }));
        }
        return stage.thenCompose(results -> {
            List<ReferenceScanSummaryV1> summaries =
                    results.stream().map(ReferenceAdapterResultV1::summary).toList();
            List<ReferenceObservationV1> observations = results.stream()
                    .flatMap(result -> result.observations().stream())
                    .sorted(Comparator.comparing(ReferenceObservationV1::kind)
                            .thenComparing(row -> row.authority().key())
                            .thenComparingLong(row -> row.coverage().inclusiveStart())
                            .thenComparingLong(row -> row.coverage().exclusiveEnd()))
                    .toList();
            ReferenceFreeProofV1 proof = M5RetentionCodecV1.finalizeProof(new ReferenceFreeProofV1(
                    request.identity(),
                    request.targetKind(),
                    request.targetIdentitySha256(),
                    request.coverage(),
                    request.selectorRoot(),
                    request.manifestRoot(),
                    request.trimRoot(),
                    request.retentionSnapshotRootSha256(),
                    M5RetentionCodecV1.calculateObservationsRoot(observations),
                    request.m4Releases(),
                    request.ownerFence(),
                    request.workerFence(),
                    request.storageFence(),
                    request.providerFence(),
                    request.auditGraceDeadlineMillis(),
                    request.observedAuthorityTimeMillis(),
                    summaries,
                    observations,
                    PLACEHOLDER));
            return freshness.requireFresh(proof).thenApply(ignored -> proof);
        });
    }

    private static void validateReferenceTarget(ReferenceProofRequestV1 request, ReferenceAdapterResultV1 result) {
        if (result.observations().stream()
                .anyMatch(row -> !row.targetIdentitySha256().equals(request.targetIdentitySha256())
                        || !row.coverage().equals(request.coverage()))) {
            throw new IllegalArgumentException("reference adapter returned a foreign target or coverage");
        }
    }

    private static Map<FloorClassV1, FloorAdapterV1> exactFloorRegistry(Map<FloorClassV1, FloorAdapterV1> adapters) {
        Objects.requireNonNull(adapters, "floorAdapters");
        if (!adapters.keySet().equals(EnumSet.allOf(FloorClassV1.class))) {
            throw new IllegalArgumentException("floor adapters do not cover the closed inventory");
        }
        EnumMap<FloorClassV1, FloorAdapterV1> copy = new EnumMap<>(FloorClassV1.class);
        for (FloorClassV1 floorClass : FloorClassV1.values()) {
            copy.put(floorClass, Objects.requireNonNull(adapters.get(floorClass), "floor adapter"));
        }
        return Map.copyOf(copy);
    }

    private static Map<ReferenceKindV1, ReferenceAdapterV1> exactReferenceRegistry(
            Map<ReferenceKindV1, ReferenceAdapterV1> adapters) {
        Objects.requireNonNull(adapters, "referenceAdapters");
        if (!adapters.keySet().equals(EnumSet.allOf(ReferenceKindV1.class))) {
            throw new IllegalArgumentException("reference adapters do not cover the closed inventory");
        }
        EnumMap<ReferenceKindV1, ReferenceAdapterV1> copy = new EnumMap<>(ReferenceKindV1.class);
        for (ReferenceKindV1 referenceKind : ReferenceKindV1.values()) {
            copy.put(referenceKind, Objects.requireNonNull(adapters.get(referenceKind), "reference adapter"));
        }
        return Map.copyOf(copy);
    }

    private static void requireSortedUniqueFloorRows(List<RetentionFloorObservationV1> rows) {
        Comparator<RetentionFloorObservationV1> order =
                Comparator.comparing(row -> row.authority().key());
        if (!rows.equals(rows.stream().sorted(order).toList())
                || rows.stream().map(row -> row.authority().key()).distinct().count() != rows.size()) {
            throw new IllegalArgumentException("floor adapter rows are not sorted unique by authority key");
        }
    }

    private static void requireSortedUniqueReferenceRows(List<ReferenceObservationV1> rows) {
        Comparator<ReferenceObservationV1> order = Comparator.comparing(
                        (ReferenceObservationV1 row) -> row.authority().key())
                .thenComparingLong(row -> row.coverage().inclusiveStart())
                .thenComparingLong(row -> row.coverage().exclusiveEnd());
        if (!rows.equals(rows.stream().sorted(order).toList())
                || rows.stream().distinct().count() != rows.size()) {
            throw new IllegalArgumentException("reference adapter rows are not sorted unique");
        }
    }

    private static <T> CompletionStage<T> requireStage(CompletionStage<T> stage, String name) {
        return Objects.requireNonNull(stage, name);
    }
}
