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

import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.BindingReadPlanBufferV1;
import com.nereusstream.storage.object.read.BindingReadPlannerV1;
import com.nereusstream.storage.object.read.BindingReadProtocolV1;
import com.nereusstream.storage.object.read.BindingReadPublicationCellV1;
import com.nereusstream.storage.object.read.BindingReadRouteTableV1;
import com.nereusstream.storage.object.read.BindingReadRouteV1;
import com.nereusstream.storage.object.read.BindingReadSourceRefV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Low-frequency selected-descriptor recovery under the existing native M4 generation lease and source planner.
 * The owner supplies its published authority reference and Cell-admitted reader; this bridge grants no selector
 * authority. Recovered caches serve ordinary protocol reads without repeating this full recovery or metadata I/O.
 */
public final class KafkaBookKeeperM4RecoveryV2 {
    /** The owner must reconcile this captured authority before installing caches into a later protocol root. */
    public record RecoveryResult(BindingReadAuthorityV1 capturedAuthority, KafkaBookKeeperReadViewV2 view) {
        public RecoveryResult {
            Objects.requireNonNull(capturedAuthority, "capturedAuthority");
            Objects.requireNonNull(view, "view");
            if (!capturedAuthority.selectedViewSha256().equals(view.descriptor().descriptorSha256())
                    || capturedAuthority.sourceGeneration() < view.descriptor().sourceGeneration()) {
                throw new IllegalArgumentException("recovered BK caches differ from their captured source authority");
            }
        }
    }

    private final AtomicReference<BindingReadAuthorityV1> current;
    private final BindingReadHazardPoolV1 hazards;
    private final BindingReadAsyncExecutorV1 executor;
    private final BindingReadPlanBufferV1 plan = new BindingReadPlanBufferV1(1);
    private final KafkaSealedBookKeeperReaderV2 reader;

    public KafkaBookKeeperM4RecoveryV2(
            AtomicReference<BindingReadAuthorityV1> current,
            BindingReadHazardPoolV1 hazards,
            Executor ownerEventLoop,
            KafkaSealedBookKeeperReaderV2 reader) {
        this.current = Objects.requireNonNull(current, "current");
        this.hazards = Objects.requireNonNull(hazards, "hazards");
        this.reader = Objects.requireNonNull(reader, "reader");
        executor = new BindingReadAsyncExecutorV1(ownerEventLoop, hazards.capacity());
    }

    /** Cancellation cannot release the source-plan lease until all started native IO has actually terminated. */
    public CompletableFuture<RecoveryResult> recover(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        var digest = descriptor.descriptorSha256();
        var binding = descriptor.sourceCut().identity().binding();
        var coverage = descriptor.sourceCut().coverage();
        var expectedRoute = route(descriptor);
        return executor.execute(current, hazards, captured -> {
            if (!captured.bindingId().equals(binding.bindingId())
                    || !captured.topicIncarnationIdentity().equals(binding.incarnationSha256())
                    || !captured.storageEpochId().digest().equals(binding.storageEpochSha256())
                    || captured.protocol() != BindingReadProtocolV1.KAFKA_OFFSET
                    || !captured.selectedViewSha256().equals(digest)
                    || captured.sourceGeneration() < descriptor.sourceGeneration()
                    || !(captured.publicationCell().protocolStateReference()
                            instanceof KafkaSealedBookKeeperDescriptorV2 selected)
                    || !selected.equals(descriptor)) {
                throw new IllegalStateException("BK recovery descriptor differs from the captured M4 authority");
            }
            var outcome = BindingReadPlannerV1.plan(
                    captured.publicationCell(),
                    coverage.inclusiveStart(),
                    coverage.exclusiveEnd(),
                    coverage.exclusiveEnd(),
                    plan);
            if (outcome != BindingReadPlannerV1.Outcome.PLANNED
                    || plan.size() != 1
                    || plan.startInclusive(0) != coverage.inclusiveStart()
                    || plan.endExclusive(0) != coverage.exclusiveEnd()
                    || !plan.route(0).equals(expectedRoute)) {
                throw new IllegalStateException("BK recovery lacks the exact complete M4 source plan");
            }
            return reader.recover(descriptor).thenApply(view -> new RecoveryResult(captured, view));
        });
    }

    /**
     * Pure projection only. The owner must install this through its exact durable-selector runtime and close its
     * local reference before a fencing transition. This method does not read, create or select metadata.
     */
    public static BindingReadAuthorityV1 project(
            KafkaSealedBookKeeperDescriptorV2 descriptor, BindingReadSelector selector) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(selector, "selector");
        var binding = descriptor.sourceCut().identity().binding();
        if (!selector.binding().equals(binding)
                || !selector.selectedViewSha256().equals(descriptor.descriptorSha256())
                || selector.sourceGeneration() < descriptor.sourceGeneration()) {
            throw new IllegalArgumentException("BK descriptor projection differs from the exact selected view");
        }
        var cell = new BindingReadPublicationCellV1(
                selector.sourceGeneration(),
                descriptor.sourceCut().coverage().exclusiveEnd(),
                0,
                new BindingReadRouteTableV1(List.of(route(descriptor))),
                descriptor);
        return new BindingReadAuthorityV1(
                binding.bindingId(),
                binding.incarnationSha256(),
                new StorageEpochId(binding.storageEpochSha256()),
                BindingReadProtocolV1.KAFKA_OFFSET,
                selector.selectedViewSha256(),
                selector.ownerEpoch(),
                selector.readAdmissionEpoch(),
                selector.admissionState() == AdmissionState.ADMITTING,
                selector.capability().generation(),
                selector.capability().evidenceSha256(),
                cell);
    }

    private static BindingReadRouteV1 route(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        var coverage = descriptor.sourceCut().coverage();
        var source = new BindingReadSourceRefV1(
                BindingReadSourceRefV1.SourceKind.BOOKKEEPER,
                descriptor.task().taskIdSha256(),
                descriptor.descriptorSha256(),
                descriptor.semanticProof().semanticValidationRootSha256(),
                0);
        return new BindingReadRouteV1(
                coverage.inclusiveStart(),
                coverage.exclusiveEnd(),
                source,
                null,
                0,
                BindingReadRouteV1.SourcePurity.KAFKA_APPEND_UNIT);
    }
}
