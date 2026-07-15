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

package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import io.oxia.testcontainers.OxiaContainer;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class F4MetadataStoreOxiaIntegrationTest {
    private static final String IMAGE = "oxia/oxia:0.16.3";

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(IMAGE)).withShards(4);

    @Test
    void exactCasPaginationAndConditionalDeleteSurviveRealRuntimeRestart() {
        String cluster = "f4/metadata/" + UUID.randomUUID();
        StreamId stream = new StreamId(F4MetadataTestValues.STREAM);
        OxiaClientConfiguration configuration = configuration();
        VersionedGenerationIndex committed;
        VersionedMaterializationStreamRegistration registration;
        VersionedPhysicalObjectRoot marked;
        VersionedReaderLease lease;
        VersionedObjectProtection protection;

        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(
                        configuration, Clock.systemUTC());
                OxiaJavaGenerationMetadataStore generations =
                        OxiaJavaGenerationMetadataStore.usingSharedRuntime(
                                configuration, runtime, Clock.systemUTC());
                OxiaJavaPhysicalObjectMetadataStore objects =
                        OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                                configuration, runtime, Clock.systemUTC())) {
            AllocatedGeneration first = generations.allocateGeneration(
                    cluster, stream, ReadView.COMMITTED,
                    new PublicationId(F4MetadataTestValues.PUBLICATION)).join();
            assertThat(generations.allocateGeneration(
                            cluster, stream, ReadView.COMMITTED,
                            new PublicationId(F4MetadataTestValues.PUBLICATION)).join())
                    .isEqualTo(first);

            GenerationIndexRecord prepared = F4MetadataTestValues.generation(GenerationLifecycle.PREPARED);
            VersionedGenerationIndex created = generations.createPrepared(cluster, prepared).join();
            committed = generations.compareAndSetIndex(
                    cluster,
                    F4MetadataTestValues.generation(GenerationLifecycle.COMMITTED),
                    created.metadataVersion()).join();
            GenerationScanPage generationPage = generations.scanIndex(
                    cluster, stream, ReadView.COMMITTED, 0, 2, Optional.empty(), 1).join();
            assertThat(generationPage.values()).containsExactly(committed);
            assertThat(generations.scanIndex(
                            cluster, stream, ReadView.COMMITTED, 0, 2,
                            generationPage.continuation(), 1).join().values())
                    .isEmpty();

            registration = generations.createOrVerifyStreamRegistration(
                    cluster, F4MetadataTestValues.registration(stream.value(), 7)).join();
            int registryShard = new F4Keyspace(cluster).materializationRegistryShard(stream);
            assertThat(generations.scanStreamRegistrations(
                            cluster, registryShard, Optional.empty(), 1).join().values())
                    .containsExactly(registration);

            PhysicalObjectRootRecord active =
                    F4MetadataTestValues.physicalRoot(PhysicalObjectLifecycle.ACTIVE);
            VersionedPhysicalObjectRoot root = objects.createRoot(cluster, active).join();
            marked = objects.compareAndSetRoot(
                    cluster, marked(active), root.metadataVersion()).join();
            lease = objects.createOrCompareReaderLease(
                    cluster, F4MetadataTestValues.readerLease()).join();
            protection = objects.createProtection(
                    cluster,
                    F4MetadataTestValues.protection(ObjectProtectionType.VISIBLE_GENERATION)).join();
            int rootShard = new F4Keyspace(cluster).physicalObjectShard(
                    new ObjectKeyHash(active.objectKeyHash()));
            assertThat(objects.scanRoots(cluster, rootShard, Optional.empty(), 10).join().values())
                    .containsExactly(marked);
        }

        GenerationIndexIdentity indexIdentity = new GenerationIndexIdentity(
                stream, ReadView.COMMITTED, committed.value().offsetEnd(), committed.value().generation());
        ObjectKeyHash object = new ObjectKeyHash(marked.value().objectKeyHash());
        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(
                        configuration, Clock.systemUTC());
                OxiaJavaGenerationMetadataStore generations =
                        OxiaJavaGenerationMetadataStore.usingSharedRuntime(
                                configuration, runtime, Clock.systemUTC());
                OxiaJavaPhysicalObjectMetadataStore objects =
                        OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                                configuration, runtime, Clock.systemUTC())) {
            assertThat(generations.getIndex(cluster, indexIdentity).join()).contains(committed);
            assertThat(generations.getStreamRegistration(cluster, stream).join()).contains(registration);
            assertThat(objects.getRoot(cluster, object).join()).contains(marked);
            assertThat(objects.scanReaderLeases(cluster, object, Optional.empty(), 10).join().values())
                    .containsExactly(lease);
            assertThat(objects.scanProtections(cluster, object, Optional.empty(), 10).join().values())
                    .containsExactly(protection);

            assertThatThrownBy(() -> generations.deleteIndex(
                            cluster, indexIdentity, committed.metadataVersion() - 1).join())
                    .satisfies(error -> assertThat(unwrap(error))
                            .isInstanceOf(F4MetadataConditionFailedException.class));
            generations.deleteIndex(cluster, indexIdentity, committed.metadataVersion()).join();
            generations.deleteStreamRegistration(
                    cluster, stream, registration.metadataVersion()).join();
            objects.deleteProtection(
                    cluster,
                    new ObjectProtectionIdentity(
                            object,
                            ObjectProtectionType.VISIBLE_GENERATION,
                            protection.value().referenceId()),
                    protection.metadataVersion()).join();
            objects.deleteReaderLease(
                    cluster, object, F4MetadataTestValues.PROCESS, lease.metadataVersion()).join();
        }
    }

    private static PhysicalObjectRootRecord marked(PhysicalObjectRootRecord value) {
        return new PhysicalObjectRootRecord(
                value.schemaVersion(), value.objectKeyHash(), value.objectKey(), value.objectId(),
                value.objectKindId(), value.objectLength(), value.storageChecksumType(),
                value.storageChecksumValue(), value.contentSha256(), value.etag(),
                PhysicalObjectLifecycle.MARKED, 2, value.createdAtMillis(), value.orphanNotBeforeMillis(),
                F4MetadataTestValues.ATTEMPT, F4MetadataTestValues.HASH_E, 300, 400,
                0, 0, 0, "", "", 0);
    }

    private static OxiaClientConfiguration configuration() {
        return new OxiaClientConfiguration(
                OXIA.getServiceAddress(),
                "default",
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                100,
                1_024);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
