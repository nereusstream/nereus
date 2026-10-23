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

package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import java.util.concurrent.CompletableFuture;

/** Actual existing resources of one Native retirement or reader; never imports or mutates membership. */
final class PulsarNativePhysicalReferenceSetV2 implements PulsarNativeControlWriterV2.PhysicalWriter {
    private final PulsarNativeBindingReadViewV1 original;
    private final PulsarNativeBindingReadViewV1 preferred;
    private final PulsarNativeReadCellBudgetV1 budget;
    private final M5BookKeeperNamespaceAuthorityV2 bookKeeper;

    private final java.util.Map<
                    com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2,
                    com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2>
            routes = new java.util.TreeMap<>();
    private final java.util.Map<
                    com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2,
                    com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1>
            enrollments = new java.util.TreeMap<>();
    private final S3PulsarOffloadObjectStoreV1.MaterializationConnection connection;
    private final com.nereusstream.storage.object.materialization.M5CellProviderProfileV2 profile;
    private final CanonicalBytes nativeOwner;
    private final Sha256Digest root;

    PulsarNativePhysicalReferenceSetV2(
            PulsarNativeBindingReadViewV1 original,
            PulsarNativeBindingReadViewV1 preferred,
            PulsarNativeReadCellBudgetV1 budget,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector predecessor,
            boolean readerLifetime) {
        this.original = original;
        this.preferred = preferred;
        this.budget = budget;
        this.bookKeeper = bookKeeper;
        try {
            connection = preferred.materializedConnectionForControl();
            profile = connection.profile();
            nativeOwner = budget.nativeOwnerIdentity(original.binding());
            var resources = new java.util.TreeSet<com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2>();
            if (readerLifetime) {
                resources.addAll(original.members());
            }
            if (!readerLifetime
                    || predecessor.mode()
                            == com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode
                                    .PREFERRED_WITH_FALLBACK) {
                resources.addAll(original.materializedMap()
                        .fallbackPhysicalReferences(
                                original.binding(), original.bookKeeperNamespaceBinding(), original.objectBinding()));
            }
            original.materializedMap()
                    .readableOutputs()
                    .forEach(value -> resources.add(value.resource().orElseThrow()));
            if (resources.isEmpty()
                    || resources.size()
                            > com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.MAX_TARGETS) {
                throw new IllegalStateException("Native fallback physical reference count is invalid");
            }
            var bk = original.binding().materializationQuota(bookKeeper).join();
            var objects = original.binding()
                    .materializationQuota(connection.physical())
                    .join();
            var bytes = new java.io.ByteArrayOutputStream();
            try (var out = new java.io.DataOutputStream(bytes)) {
                out.writeInt(0x4e504d52); // NPMR
                out.writeInt(1);
                out.write(Sha256Digest.hash(nativeOwner).bytes().toByteArray());
                out.write(original.sha256().bytes().toByteArray());
                out.write(preferred.sha256().bytes().toByteArray());
                out.write(Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(predecessor))
                        .bytes()
                        .toByteArray());
                out.write(Sha256Digest.hash(profile.encode()).bytes().toByteArray());
                for (var resource : resources) {
                    boolean object = resource
                            instanceof com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectVersion;
                    var route = object ? objects : bk;
                    routes.put(resource, route);
                    var fact = route.readResourceFact(resource)
                            .toCompletableFuture()
                            .join();
                    var authority = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                            fact.authority().canonicalStoredBytes());
                    requireEnrollment(
                            resource,
                            authority,
                            object
                                    ? profile.providerConfigurationSha256()
                                    : bookKeeper.capability().configurationDigest());
                    enrollments.put(resource, authority.writerEnrollment());
                    out.write(resource.sha256().bytes().toByteArray());
                    out.write(authority
                            .writerEnrollment()
                            .capabilitySetRootSha256()
                            .bytes()
                            .toByteArray());
                    out.write(authority
                            .writerEnrollment()
                            .implementationRootSha256()
                            .bytes()
                            .toByteArray());
                    out.write(authority
                            .writerEnrollment()
                            .policyRootSha256()
                            .bytes()
                            .toByteArray());
                }
            }
            root = Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
            admit().join();
        } catch (java.io.IOException failure) {
            throw new java.util.concurrent.CompletionException(failure);
        }
    }

    private void requireEnrollment(
            com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2 resource,
            com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1 actual,
            Sha256Digest capability) {
        if (!actual.target().resourceId().equals(resource)
                || actual.state()
                        != com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                .TargetDeleteAuthorityStateV1.OPEN_V1
                || !actual.writerEnrollment().capabilitySetRootSha256().equals(capability)
                || enrollments.containsKey(resource)
                        && !enrollments.get(resource).equals(actual.writerEnrollment())
                || resource instanceof com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectVersion
                        && !actual.writerEnrollment().policyRootSha256().equals(Sha256Digest.hash(profile.encode()))) {
            throw new IllegalStateException("Native fallback physical enrollment changed or is fenced");
        }
    }

    public Sha256Digest root() {
        return root;
    }

    public long writerDescriptorBytes() {
        return Math.multiplyExact(routes.size(), 512L);
    }

    public CompletableFuture<Void> reconcile() {
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> admit() {
        var work = budget.requireNativeOwner(original.binding(), nativeOwner)
                .thenCompose(ignored -> original.binding().revalidate())
                .thenCompose(ignored -> connection.requireProfileCurrent());
        for (var item : routes.entrySet()) {
            var resource = item.getKey();
            var route = item.getValue();
            boolean object =
                    resource instanceof com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectVersion;
            var capability = object
                    ? profile.providerConfigurationSha256()
                    : bookKeeper.capability().configurationDigest();
            var member = new com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member(
                    com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain
                            .PULSAR_ENTRY,
                    object
                            ? profile.scopeId().digest()
                            : bookKeeper.capability().providerScopeId().digest(),
                    original.bindingIdentity());
            work = work.thenCompose(ignored -> route.readResourceFact(resource))
                    .thenCompose(fact -> {
                        requireEnrollment(
                                resource,
                                com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                        fact.authority().canonicalStoredBytes()),
                                capability);
                        return route.members().readFact(resource);
                    })
                    .thenAccept(fact -> {
                        if (!fact.inventory().protocolCell().equals(profile.protocolCellSha256())
                                || !fact.inventory().members().contains(member)
                                || !preferred.isCurrent()) {
                            throw new IllegalStateException(
                                    "Native fallback physical member or preferred source changed");
                        }
                    });
        }
        return work;
    }

    public com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2 writerGuard() {
        var nativeRoutes = routes.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        value -> value.getKey().authorityKey(), java.util.Map.Entry::getValue));
        return new com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2(
                new com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1(
                        new PulsarNativeMetadataWriterV2.PhysicalRoutes(nativeRoutes)));
    }

    public java.util.Map<
                    com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2,
                    com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context>
            writerContexts(
                    Sha256Digest mutationRoot,
                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1 kind) {
        var writerClass = kind
                        == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                .READ_GENERATION_PIN_OR_OPEN_HANDLE
                ? com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1
                        .OWNER_WORKER_LEASE_HANDLE_PIN_V1
                : kind
                                == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                        .MANIFEST_SELECTED
                        ? com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1
                                .MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1
                        : com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1
                                .M4_SOURCE_PROTECTION_RELEASE_V1;
        return enrollments.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        java.util.Map.Entry::getKey,
                        value -> new com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context(
                                writerClass,
                                value.getValue().capabilitySetRootSha256(),
                                Sha256Digest.hash(nativeOwner),
                                mutationRoot)));
    }
}
