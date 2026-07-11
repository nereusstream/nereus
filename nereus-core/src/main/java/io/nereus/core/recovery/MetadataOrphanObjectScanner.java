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

package io.nereus.core.recovery;

import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.api.ObjectId;
import io.nereus.metadata.oxia.OxiaMetadataStore;
import io.nereus.metadata.oxia.records.ObjectManifestRecord;
import io.nereus.metadata.oxia.records.ObjectReferenceRecord;
import io.nereus.metadata.oxia.records.StreamSliceManifestRecord;
import io.nereus.metadata.oxia.records.VisibleSliceReferenceRecord;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/** Metadata implementation that repairs references before classifying an object. */
public final class MetadataOrphanObjectScanner implements OrphanObjectScanner {
    private final String cluster;
    private final OxiaMetadataStore metadataStore;
    private final RecoveryMetricsObserver observer;
    private final Executor callbackExecutor;
    private final AtomicBoolean closed = new AtomicBoolean();

    public MetadataOrphanObjectScanner(
            String cluster,
            OxiaMetadataStore metadataStore,
            RecoveryMetricsObserver observer,
            Executor callbackExecutor) {
        this.cluster = requireNonBlank(cluster, "cluster");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.observer = Objects.requireNonNull(observer, "observer");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
    }

    @Override
    public CompletableFuture<OrphanObjectAssessment> scan(ObjectId objectId) {
        if (closed.get()) {
            return NereusException.failedFuture(
                    ErrorCode.STORAGE_CLOSED, false, "orphan object scanner is closed");
        }
        if (objectId == null) {
            return NereusException.failedFuture(
                    ErrorCode.INVALID_ARGUMENT, false, "objectId is required");
        }
        return metadataStore.getObjectManifest(cluster, objectId)
                .thenComposeAsync(manifest -> manifest
                        .map(value -> inspectManifest(objectId, value))
                        .orElseGet(() -> CompletableFuture.completedFuture(missingManifest(objectId))),
                        callbackExecutor);
    }

    private CompletableFuture<OrphanObjectAssessment> inspectManifest(
            ObjectId objectId,
            ObjectManifestRecord manifest) {
        if (!manifest.objectId().equals(objectId.value())) {
            return NereusException.failedFuture(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "object manifest id does not match its lookup key");
        }
        return metadataStore.repairObjectReferences(cluster, objectId)
                .thenApplyAsync(references -> assess(manifest, references), callbackExecutor);
    }

    private OrphanObjectAssessment assess(
            ObjectManifestRecord manifest,
            ObjectReferenceRecord references) {
        if (!manifest.objectId().equals(references.objectId())) {
            throw invariant("object references belong to a different manifest");
        }
        Map<SliceIdentity, StreamSliceManifestRecord> manifestSlices = new HashMap<>();
        for (StreamSliceManifestRecord slice : manifest.slices()) {
            manifestSlices.put(new SliceIdentity(slice.streamId(), slice.sliceId()), slice);
        }
        for (VisibleSliceReferenceRecord visible : references.visibleSlices()) {
            StreamSliceManifestRecord slice = manifestSlices.get(
                    new SliceIdentity(visible.streamId(), visible.sliceId()));
            if (slice == null) {
                throw invariant("object references contain a slice absent from the manifest");
            }
        }
        int manifestCount = manifest.slices().size();
        int reachableCount = references.visibleSlices().size();
        OrphanObjectStatus status;
        if (reachableCount == 0) {
            status = OrphanObjectStatus.UNREFERENCED_MANIFEST;
        } else if (reachableCount == manifestCount) {
            status = OrphanObjectStatus.FULLY_REFERENCED;
        } else {
            status = OrphanObjectStatus.PARTIALLY_REFERENCED;
        }
        OrphanObjectAssessment assessment = new OrphanObjectAssessment(
                new ObjectId(manifest.objectId()),
                status,
                manifestCount,
                reachableCount,
                manifest.orphanExpiresAtMillis());
        observe(() -> observer.onObjectReferenceRepair(reachableCount));
        observe(() -> observer.onObjectAssessed(status));
        return assessment;
    }

    private OrphanObjectAssessment missingManifest(ObjectId objectId) {
        OrphanObjectAssessment assessment = new OrphanObjectAssessment(
                objectId,
                OrphanObjectStatus.MISSING_MANIFEST,
                0,
                0,
                0);
        observe(() -> observer.onObjectAssessed(assessment.status()));
        return assessment;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }

    private static void observe(Runnable callback) {
        try {
            callback.run();
        } catch (RuntimeException ignored) {
            // Metrics callbacks cannot alter recovery classification.
        }
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private record SliceIdentity(String streamId, String sliceId) {
    }
}
