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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BindingTrimFrontierV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Revisioned deletion evidence, never physical identity. Native adapters must verify every bound authority fact. */
public record DeleteEligibilitySnapshotV2(
        PhysicalResourceIdV2 resource,
        ReclamationReason reason,
        long generation,
        AuthorityFactV1 namespaceAdmission,
        AuthorityFactV1 completeMemberInventory,
        List<MemberSnapshot> members) {
    public static final int MAX_MEMBERS = 64;
    public static final int MAX_REPLACEMENTS = 256;
    public static final int MAX_SNAPSHOT_BYTES = 786432;

    public enum ReclamationReason {
        REPLACED_REPRESENTATION,
        LOGICAL_EXPIRY,
        UNPUBLISHED_ARTIFACT
    }

    public enum ReferenceScope {
        LOGICAL_OBLIGATION,
        OLD_PHYSICAL_REFERENCE,
        BOTH
    }

    public enum SemanticAspect {
        READ_RESULTS,
        RECOVERY,
        PRODUCER_STATE,
        TRANSACTIONS,
        LEADER_EPOCH,
        TIMESTAMP_LOOKUP,
        INDEX_COVERAGE,
        NATIVE_SOURCE_AUTHORITY
    }

    public enum NativeDisposition {
        REPLACEMENT_ALLOWED,
        LOGICAL_EXPIRY_ALLOWED,
        UNPUBLISHED_CLEANUP_ALLOWED,
        RETAIN
    }

    /** Equal normalized semantics must be established by a protocol verifier, not by copying a root. */
    public record SemanticTransfer(
            SemanticAspect aspect,
            ProtocolCoverage coverage,
            Sha256Digest priorSemanticRoot,
            Sha256Digest replacementSemanticRoot,
            AuthorityFactV1 verifierAuthority) {
        public SemanticTransfer {
            Objects.requireNonNull(aspect, "aspect");
            Objects.requireNonNull(coverage, "coverage");
            requireDigest(priorSemanticRoot, "priorSemanticRoot");
            requireDigest(replacementSemanticRoot, "replacementSemanticRoot");
            Objects.requireNonNull(verifierAuthority, "verifierAuthority");
            if (!priorSemanticRoot.equals(replacementSemanticRoot)) {
                throw new IllegalArgumentException("replacement changed an applicable protocol semantic obligation");
            }
        }
    }

    public record ReplacementEvidence(
            AuthorityFactV1 selectedGeneration,
            List<PhysicalResourceIdV2> replacementResources,
            List<SemanticTransfer> transfers) {
        public ReplacementEvidence {
            Objects.requireNonNull(selectedGeneration, "selectedGeneration");
            replacementResources = List.copyOf(Objects.requireNonNull(replacementResources, "replacementResources"));
            if (replacementResources.isEmpty()
                    || replacementResources.size() > MAX_REPLACEMENTS
                    || !replacementResources.equals(
                            replacementResources.stream().sorted().distinct().toList())) {
                throw new IllegalArgumentException("replacement resources must be bounded, sorted and unique");
            }
            transfers = List.copyOf(Objects.requireNonNull(transfers, "transfers"));
            if (transfers.isEmpty()
                    || transfers.size() > SemanticAspect.values().length
                    || !transfers.equals(transfers.stream()
                            .sorted(Comparator.comparing(SemanticTransfer::aspect))
                            .toList())
                    || transfers.stream()
                                    .map(SemanticTransfer::aspect)
                                    .distinct()
                                    .count()
                            != transfers.size()) {
                throw new IllegalArgumentException("semantic transfers must be bounded, sorted and unique");
            }
        }
    }

    /** Exact terminal task and no-adoption evidence; age or an empty local queue cannot instantiate this proof. */
    public record UnpublishedEvidence(
            Sha256Digest taskAttemptId,
            AuthorityFactV1 terminalTask,
            AuthorityFactV1 fencedTaskOwner,
            AuthorityFactV1 closedAdoptionAndResponseLossPaths,
            Optional<AuthorityFactV1> neverAdmittedProtection) {
        public UnpublishedEvidence {
            requireDigest(taskAttemptId, "taskAttemptId");
            Objects.requireNonNull(terminalTask, "terminalTask");
            Objects.requireNonNull(fencedTaskOwner, "fencedTaskOwner");
            Objects.requireNonNull(closedAdoptionAndResponseLossPaths, "closedAdoptionAndResponseLossPaths");
            neverAdmittedProtection = Objects.requireNonNull(neverAdmittedProtection, "neverAdmittedProtection");
        }
    }

    /** Floors remain logical observations; every reference-proof row specifically denotes the old physical resource. */
    public record MemberSnapshot(
            RetentionFloorSnapshotV1 floors,
            ReferenceFreeProofV1 physicalReferences,
            AuthorityFactV1 nativeAuthority,
            NativeDisposition nativeDisposition,
            Optional<ReplacementEvidence> replacement,
            Optional<BindingTrimFrontierV1> expiry,
            Optional<UnpublishedEvidence> unpublished) {
        public MemberSnapshot {
            Objects.requireNonNull(floors, "floors");
            Objects.requireNonNull(physicalReferences, "physicalReferences");
            Objects.requireNonNull(nativeAuthority, "nativeAuthority");
            Objects.requireNonNull(nativeDisposition, "nativeDisposition");
            replacement = Objects.requireNonNull(replacement, "replacement");
            expiry = Objects.requireNonNull(expiry, "expiry");
            unpublished = Objects.requireNonNull(unpublished, "unpublished");
            if ((replacement.isPresent() ? 1 : 0) + (expiry.isPresent() ? 1 : 0) + (unpublished.isPresent() ? 1 : 0)
                    != 1) {
                throw new IllegalArgumentException("member must have exactly one reclamation reason proof");
            }
            M5RetentionCodecV1.encodeSnapshot(floors);
            M5RetentionCodecV1.encodeReferenceFreeProof(physicalReferences);
            if (!floors.identity().equals(physicalReferences.identity())
                    || floors.domain() != physicalReferences.coverage().domain()
                    || !floors.snapshotRootSha256().equals(physicalReferences.retentionSnapshotRootSha256())
                    || !floors.ownerFence().equals(physicalReferences.ownerFence())
                    || !floors.storageFence().equals(physicalReferences.storageFence())) {
                throw new IllegalArgumentException(
                        "logical floors and physical proof do not share exact identity/fences");
            }
            physicalReferences.m4Releases().forEach(release -> {
                if (!M4ReadControlCodecV1.decodeProtection(release.canonicalProtectionBytes())
                        .binding()
                        .equals(floors.identity().binding())) {
                    throw new IllegalArgumentException("M4 release belongs to another Binding");
                }
            });
        }
    }

    public DeleteEligibilitySnapshotV2 {
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(reason, "reason");
        if (generation <= 0) {
            throw new IllegalArgumentException("eligibility generation must be positive");
        }
        Objects.requireNonNull(namespaceAdmission, "namespaceAdmission");
        Objects.requireNonNull(completeMemberInventory, "completeMemberInventory");
        members = List.copyOf(Objects.requireNonNull(members, "members"));
        if (members.isEmpty()
                || members.size() > MAX_MEMBERS
                || !members.equals(members.stream()
                        .sorted(Comparator.comparing(DeleteEligibilitySnapshotV2::memberKey))
                        .toList())
                || members.stream()
                                .map(DeleteEligibilitySnapshotV2::memberKey)
                                .distinct()
                                .count()
                        != members.size()) {
            throw new IllegalArgumentException("member inventory must be bounded, sorted and unique");
        }
        Sha256Digest cell = members.get(0).floors().identity().protocolCellSha256();
        for (MemberSnapshot member : members) {
            if (!member.physicalReferences().targetIdentitySha256().equals(resource.sha256())
                    || !member.floors().identity().protocolCellSha256().equals(cell)) {
                throw new IllegalArgumentException(
                        "foreign physical resource or cross-Cell member in deletion snapshot");
            }
            validateReason(resource, reason, member);
        }
    }

    public Sha256Digest sha256() {
        return Sha256Digest.hash(DeleteEligibilityCodecV2.encode(this));
    }

    /** Complete exact vector; caller must reread it under enrolled writer/native fencing before CAS-1 and CAS-2. */
    public List<AuthorityFactV1> authorityFacts() {
        List<AuthorityFactV1> facts = new ArrayList<>();
        facts.add(namespaceAdmission);
        facts.add(completeMemberInventory);
        for (MemberSnapshot member : members) {
            facts.add(member.nativeAuthority());
            member.floors().rows().forEach(row -> facts.add(row.authority()));
            ReferenceFreeProofV1 proof = member.physicalReferences();
            facts.addAll(List.of(
                    proof.selectorRoot(),
                    proof.manifestRoot(),
                    proof.trimRoot(),
                    proof.ownerFence(),
                    proof.workerFence(),
                    proof.storageFence(),
                    proof.providerFence()));
            proof.m4Releases().forEach(release -> facts.add(release.protectionAuthority()));
            proof.observations().forEach(row -> facts.add(row.authority()));
            member.replacement().ifPresent(value -> {
                facts.add(value.selectedGeneration());
                value.transfers().forEach(transfer -> facts.add(transfer.verifierAuthority()));
            });
            member.unpublished().ifPresent(value -> {
                facts.addAll(List.of(
                        value.terminalTask(), value.fencedTaskOwner(), value.closedAdoptionAndResponseLossPaths()));
                value.neverAdmittedProtection().ifPresent(facts::add);
            });
        }
        var byKey = new java.util.TreeMap<String, AuthorityFactV1>();
        for (AuthorityFactV1 fact : facts) {
            AuthorityFactV1 previous = byKey.putIfAbsent(fact.key(), fact);
            if (previous != null && !previous.equals(fact)) {
                throw new IllegalArgumentException("eligibility has conflicting authority facts for one key");
            }
        }
        return List.copyOf(byKey.values());
    }

    public static ReferenceScope floorScope(FloorClassV1 kind) {
        return switch (Objects.requireNonNull(kind, "kind")) {
            case BINDING_EPOCH_POLICY, KAFKA_CONSUMER_GROUP -> ReferenceScope.LOGICAL_OBLIGATION;
            case KAFKA_PRODUCER_TRANSACTION,
                    KAFKA_REPLICATION_RECOVERY,
                    PULSAR_CURSOR_SUBSCRIPTION,
                    GENERATION_READ,
                    SHARED_PHYSICAL_SOURCE,
                    PROJECTION_MIGRATION -> ReferenceScope.BOTH;
            case LIFECYCLE_TASK, AUDIT_GRACE -> ReferenceScope.OLD_PHYSICAL_REFERENCE;
        };
    }

    public static ReferenceScope referenceScope(ReferenceKindV1 kind) {
        return switch (Objects.requireNonNull(kind, "kind")) {
            case KAFKA_GROUP_RETENTION -> ReferenceScope.LOGICAL_OBLIGATION;
            case KAFKA_PRODUCER_RECOVERY,
                    KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                    KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY,
                    PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR,
                    RECOVERY_CHECKPOINT_OR_SNAPSHOT,
                    PROJECTION_MIGRATION_OR_EXPORT -> ReferenceScope.BOTH;
            case MANIFEST_SELECTED,
                    MANIFEST_FALLBACK,
                    READ_GENERATION_PIN_OR_OPEN_HANDLE,
                    SOURCE_PROTECTION,
                    MATERIALIZATION_OR_COMPACTION_TASK,
                    RETIREMENT_OR_DELETE_RECONCILIATION,
                    SHARED_PHYSICAL_MEMBER,
                    AUDIT_GRACE -> ReferenceScope.OLD_PHYSICAL_REFERENCE;
        };
    }

    public static EnumSet<SemanticAspect> requiredSemantics(PositionDomain domain) {
        return Objects.requireNonNull(domain, "domain") == PositionDomain.KAFKA_OFFSET
                ? EnumSet.allOf(SemanticAspect.class)
                : EnumSet.of(
                        SemanticAspect.READ_RESULTS,
                        SemanticAspect.RECOVERY,
                        SemanticAspect.INDEX_COVERAGE,
                        SemanticAspect.NATIVE_SOURCE_AUTHORITY);
    }

    private static void validateReason(PhysicalResourceIdV2 resource, ReclamationReason reason, MemberSnapshot member) {
        ReferenceFreeProofV1 proof = member.physicalReferences();
        if (reason != ReclamationReason.UNPUBLISHED_ARTIFACT
                && (proof.targetKind() != ReferenceTargetKindV1.READABLE_SOURCE
                        || proof.m4Releases().isEmpty())) {
            throw new IllegalArgumentException("published resource requires exact M4 RELEASED source proof");
        }
        switch (reason) {
            case REPLACED_REPRESENTATION -> {
                ReplacementEvidence replacement = member.replacement()
                        .orElseThrow(
                                () -> new IllegalArgumentException("replacement reason lacks replacement evidence"));
                if (member.nativeDisposition() != NativeDisposition.REPLACEMENT_ALLOWED
                        || !replacement.selectedGeneration().equals(proof.manifestRoot())
                        || replacement.replacementResources().contains(resource)) {
                    throw new IllegalArgumentException(
                            "replacement is not selected, native-authorized or physically distinct");
                }
                EnumSet<SemanticAspect> actual = EnumSet.noneOf(SemanticAspect.class);
                for (SemanticTransfer transfer : replacement.transfers()) {
                    actual.add(transfer.aspect());
                    if (!transfer.coverage().equals(proof.coverage())) {
                        throw new IllegalArgumentException(
                                "semantic replacement does not cover the complete resource range");
                    }
                }
                if (!actual.equals(requiredSemantics(proof.coverage().domain()))) {
                    throw new IllegalArgumentException(
                            "replacement omits an applicable read/recovery/index obligation");
                }
                // Logical floor values remain retained obligations served by the replacement; they need not expire.
            }
            case LOGICAL_EXPIRY -> {
                BindingTrimFrontierV1 trim = member.expiry()
                        .orElseThrow(() -> new IllegalArgumentException("expiry reason lacks typed trim evidence"));
                if (member.nativeDisposition() != NativeDisposition.LOGICAL_EXPIRY_ALLOWED
                        || !trim.identity()
                                .binding()
                                .equals(member.floors().identity().binding())
                        || trim.domain() != proof.coverage().domain()
                        || trim.newFrontier() < proof.coverage().exclusiveEnd()
                        || member.floors().minimumSafeFloor() < trim.newFrontier()
                        || !trim.retentionPolicyRootSha256()
                                .equals(member.floors().retentionPolicyRootSha256())
                        || !Sha256Digest.hash(M5RetentionCodecV1.encodeTrimFrontier(trim))
                                .equals(proof.trimRoot().valueSha256())) {
                    throw new IllegalArgumentException("logical expiry lacks exact full-range trim and every floor");
                }
            }
            case UNPUBLISHED_ARTIFACT -> {
                UnpublishedEvidence unpublished = member.unpublished()
                        .orElseThrow(() ->
                                new IllegalArgumentException("unpublished reason lacks fenced terminal task evidence"));
                if (member.nativeDisposition() != NativeDisposition.UNPUBLISHED_CLEANUP_ALLOWED
                        || proof.targetKind() != ReferenceTargetKindV1.UNSELECTED_OUTPUT
                        || (proof.m4Releases().isEmpty()
                                && unpublished.neverAdmittedProtection().isEmpty())) {
                    throw new IllegalArgumentException(
                            "unpublished output still lacks task/adoption/protection closure");
                }
            }
        }
    }

    private static String memberKey(MemberSnapshot member) {
        var binding = member.floors().identity().binding();
        return binding.bindingId().digest().toHex()
                + binding.incarnationSha256().toHex()
                + binding.storageEpochSha256().toHex();
    }

    private static void requireDigest(Sha256Digest digest, String name) {
        M5TargetDeleteAuthorityRecordsV1.requireDigest(digest, name);
    }
}
