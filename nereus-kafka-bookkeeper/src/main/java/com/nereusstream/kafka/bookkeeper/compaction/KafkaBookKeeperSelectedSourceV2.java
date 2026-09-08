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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.InputBatch;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Captures the current native selected BK generation, including all data/index ledgers and gap-only output.
 * SourceExtent's canonical body is its immutable descriptor; input batches come from independently verified parts.
 * The supplied stores must be the admitted native Binding route. Protocol semantics and M4 fallback retirement
 * remain owner obligations. This reader never changes selection or clears tickets belonging to an older process.
 */
public final class KafkaBookKeeperSelectedSourceV2 implements KafkaBookKeeperPublicationTicketsV2.InputMembership {
    public record Bounds(int entries, int batches, int records, int encodedBytes, long decodedBytes) {
        public Bounds {
            if (entries < 1
                    || entries > 131072
                    || batches < 0
                    || batches > KafkaCompactionRecordsV1.MAX_BATCHES
                    || records < 0
                    || records > KafkaCompactionRecordsV1.MAX_RECORDS
                    || encodedBytes < 1
                    || encodedBytes > 256 * 1024 * 1024
                    || decodedBytes < 0
                    || decodedBytes > 256L * 1024 * 1024) {
                throw new IllegalArgumentException("selected source budget exceeds its hard bounds");
            }
        }
    }

    public record Snapshot(
            BindingReadSelector selector,
            SourceExtent extent,
            List<InputBatch> batches,
            List<PhysicalResourceIdV2> resources,
            KafkaBookKeeperReadViewV2 view) {
        public Snapshot {
            batches = List.copyOf(batches);
            resources = List.copyOf(resources);
        }
    }

    private record Selected(BindingReadSelector selector, KafkaSealedBookKeeperDescriptorV2 descriptor) {}

    private record ReadOutcome(Snapshot snapshot, Throwable failure) {}

    private final CanonicalControlMetadataStore store;
    private final M4ReadControlCoordinatorV1 m4;
    private final M5BookKeeperNativeCreateClientV2 client;
    private final M5TargetDeleteMultiWriterGuardV2 guard;
    private final Bounds bounds;
    private final Executor owner;

    public KafkaBookKeeperSelectedSourceV2(
            CanonicalControlMetadataStore store,
            M4ReadControlCoordinatorV1 m4,
            M5BookKeeperNativeCreateClientV2 client,
            M5TargetDeleteMultiWriterGuardV2 guard,
            Bounds bounds,
            Executor owner) {
        this.store = Objects.requireNonNull(store, "store");
        this.m4 = Objects.requireNonNull(m4, "m4");
        this.client = Objects.requireNonNull(client, "client");
        this.guard = Objects.requireNonNull(guard, "guard");
        this.bounds = Objects.requireNonNull(bounds, "bounds");
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    public CompletionStage<Snapshot> capture() {
        var work = CompletableFuture.supplyAsync(this::selected, owner).thenCompose(selected -> {
            var descriptor = selected.descriptor();
            List<PhysicalResourceIdV2> resources = descriptor.sealedParts().stream()
                    .map(part -> (PhysicalResourceIdV2) new PhysicalResourceIdV2.BookKeeperLedger(
                            descriptor.task().namespace(),
                            part.handle().ledgerIdentity().ledgerId()))
                    .toList();
            var context = new Context(
                    ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1,
                    client.capabilitySnapshot().configurationDigest(),
                    Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selected.selector())),
                    descriptor.descriptorSha256());
            return guard.execute(resources, context, () -> {
                        var reads = client.newSession();
                        CompletionStage<Snapshot> scan;
                        try {
                            var reader = new KafkaSealedBookKeeperReaderV2(
                                    reads,
                                    client::captureExactTarget,
                                    bounds.encodedBytes(),
                                    new KafkaSealedBookKeeperReaderV2.DecodingBounds(
                                            bounds.records(), bounds.decodedBytes()),
                                    owner);
                            scan = reader.recover(descriptor)
                                    .thenApplyAsync(
                                            view -> {
                                                if (!selected.equals(selected())) {
                                                    throw new IllegalStateException(
                                                            "selected source changed during native capture");
                                                }
                                                return snapshot(selected, resources, view);
                                            },
                                            owner);
                        } catch (RuntimeException failure) {
                            scan = CompletableFuture.failedFuture(failure);
                        }
                        return scan.handle(ReadOutcome::new).thenCompose(outcome -> reads.closeAsync()
                                .thenApply(ignored -> new Completion<>(
                                        outcome,
                                        Optional.of(Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(36)
                                                .putInt(0x4d355343)
                                                .put(descriptor
                                                        .descriptorSha256()
                                                        .bytes()
                                                        .toByteArray())
                                                .array()))))));
                    })
                    .thenCompose(result -> {
                        if (!result.mutationInvoked() || result.value().isEmpty()) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("selected source physical admission failed"));
                        }
                        if (!result.unresolvedTargets().isEmpty()) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("selected source ticket release is unresolved"));
                        }
                        var outcome = result.value().orElseThrow();
                        return outcome.failure() == null
                                ? CompletableFuture.completedFuture(outcome.snapshot())
                                : CompletableFuture.failedFuture(outcome.failure());
                    });
        });
        // Observer cancellation cannot abandon accepted reads, owned session close or this invocation's tickets.
        return work.thenApply(value -> value);
    }

    @Override
    public CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(CompactionPlan plan) {
        Objects.requireNonNull(plan, "plan");
        var cut = plan.sourceCut();
        if (cut.sources().size() != 1 || cut.sources().get(0).kind() != SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("selected source requires one complete BK generation"));
        }
        return capture().thenApply(snapshot -> {
            if (!snapshot.extent().equals(cut.sources().get(0))
                    || !snapshot.selector().equals(cut.predecessorSelector())
                    || !snapshot.selector().binding().equals(cut.identity().binding())
                    || !client.capabilitySnapshot()
                            .providerScopeId()
                            .digest()
                            .equals(cut.identity().providerScopeSha256())
                    || !snapshot.batches().equals(plan.inputBatches())) {
                throw new IllegalArgumentException("selected source differs from complete compaction input plan");
            }
            return Map.of(snapshot.extent().sourceIdentitySha256(), snapshot.resources());
        });
    }

    private Selected selected() {
        var selector = m4.readSelector().orElseThrow(() -> new IllegalStateException("selected source lacks selector"));
        if (selector.admissionState() != AdmissionState.ADMITTING) {
            throw new IllegalStateException("selected source read admission is stopped");
        }
        var key = KafkaBookKeeperCompactionPublicationV2.descriptorKey(selector.selectedViewSha256());
        var descriptor = KafkaSealedBookKeeperDescriptorCodecV2.decode(
                store.get(key).orElseThrow(() -> new IllegalStateException("selected source descriptor is absent")));
        if (!descriptor.descriptorSha256().equals(selector.selectedViewSha256())
                || !descriptor.sourceCut().identity().binding().equals(selector.binding())
                || descriptor.sourceGeneration() > selector.sourceGeneration()
                || !descriptor.task().namespace().equals(client.spec().namespace())
                || !descriptor.task().capability().equals(client.capabilitySnapshot())) {
            throw new IllegalStateException("selected source descriptor differs from its native route");
        }
        long entries = 0;
        long bytes = 0;
        for (int part = 0; part < descriptor.task().parts().size(); part++) {
            if (!client.spec().configurations().contains(descriptor.task().configuration(part))) {
                throw new IllegalStateException("selected source configuration is not admitted by its client");
            }
            entries += descriptor.task().parts().get(part).entryCount();
            bytes += descriptor.task().parts().get(part).length();
        }
        if (entries > bounds.entries() || bytes > bounds.encodedBytes() || descriptor.batchCount() > bounds.batches()) {
            throw new IllegalArgumentException("selected source physical budget exhausted before reads");
        }
        return new Selected(selector, descriptor);
    }

    private static Snapshot snapshot(
            Selected selected, List<PhysicalResourceIdV2> resources, KafkaBookKeeperReadViewV2 view) {
        var descriptor = selected.descriptor();
        var canonical = descriptor.encode();
        var identity = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(36)
                .putInt(0x4d355347)
                .put(descriptor.descriptorSha256().bytes().toByteArray())
                .array()));
        int records = 0;
        long minimum = Long.MAX_VALUE;
        long maximum = -1;
        var inputs = new ArrayList<InputBatch>();
        for (var batch : view.parsedBatches()) {
            inputs.add(new InputBatch(identity, inputs.size(), batch.canonicalBody()));
            for (var record : batch.records()) {
                records = Math.addExact(records, 1);
                minimum = Math.min(minimum, record.timestamp());
                maximum = Math.max(maximum, record.timestamp());
            }
        }
        var extent = new SourceExtent(
                SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2,
                identity,
                descriptor.sourceCut().coverage(),
                KafkaBookKeeperCompactionPublicationV2.descriptorKey(descriptor.descriptorSha256()),
                canonical.length(),
                records,
                records == 0 ? -1 : minimum,
                records == 0 ? -1 : maximum,
                Sha256Digest.hash(canonical),
                Optional.empty(),
                Optional.empty(),
                descriptor.descriptorSha256(),
                Sha256Digest.hash(CanonicalUtf8.fromString("BK_COMPACTED_V2_PLAINTEXT/"
                                + descriptor.task().capability().credentialIdentityVersion())
                        .bytes()),
                true,
                true,
                List.of(selected.selector().binding().bindingId().digest()));
        return new Snapshot(
                selected.selector(),
                extent,
                inputs,
                M5TargetDeleteMultiWriterGuardV2.canonicalTargets(resources),
                view);
    }
}
