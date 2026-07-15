/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import static com.nereusstream.metadata.oxia.retirement.RetirementMetadataStoreTestSupport.CLUSTER;
import static com.nereusstream.metadata.oxia.retirement.RetirementMetadataStoreTestSupport.OBJECT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.PartitionKey;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.ObjectReferenceRecord;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class ObjectAuditRetirementStoreContractTest {
    private static final ObjectId OBJECT = new ObjectId(OBJECT_ID);
    private static final Checksum WRONG_DIGEST = new Checksum(ChecksumType.SHA256, "f".repeat(64));

    @Test
    void loadsHydratedExactAuditsAndDeletesReferencesBeforeManifest() {
        RetirementMetadataStoreTestSupport.FakeClient client = new RetirementMetadataStoreTestSupport.FakeClient();
        ObjectAuditRetirementStore store = new OxiaJavaObjectAuditRetirementStore(client);
        OxiaKeyspace keys = new OxiaKeyspace(CLUSTER);
        PartitionKey partition = keys.objectPartitionKey(OBJECT);
        String referencesKey = keys.objectReferencesKey(OBJECT);
        String manifestKey = keys.objectManifestKey(OBJECT);
        ObjectReferenceRecord references = RetirementMetadataStoreTestSupport.references(OBJECT_ID);
        ObjectManifestRecord manifest = RetirementMetadataStoreTestSupport.manifest(OBJECT_ID);
        client.put(referencesKey, partition, references, ObjectReferenceRecord.class, 81);
        client.put(manifestKey, partition, manifest, ObjectManifestRecord.class, 83);

        VersionedObjectReferencesAudit capturedReferences = store.getReferences(
                CLUSTER, OBJECT).join().orElseThrow();
        VersionedObjectManifestAudit capturedManifest = store.getManifest(
                CLUSTER, OBJECT).join().orElseThrow();
        assertThat(capturedReferences.value().metadataVersion()).isEqualTo(81);
        assertThat(capturedManifest.value().metadataVersion()).isEqualTo(83);
        assertThat(capturedReferences.durableValueSha256())
                .isEqualTo(RetirementMetadataStoreTestSupport.digest(
                        references, ObjectReferenceRecord.class));
        assertThat(capturedManifest.durableValueSha256())
                .isEqualTo(RetirementMetadataStoreTestSupport.digest(
                        manifest, ObjectManifestRecord.class));

        store.deleteReferences(
                CLUSTER,
                OBJECT,
                capturedReferences.metadataVersion(),
                capturedReferences.durableValueSha256()).join();
        assertThat(client.contains(referencesKey, partition)).isFalse();
        assertThat(client.contains(manifestKey, partition)).isTrue();
        store.deleteManifest(
                CLUSTER,
                OBJECT,
                capturedManifest.metadataVersion(),
                capturedManifest.durableValueSha256()).join();
        assertThat(client.contains(manifestKey, partition)).isFalse();
    }

    @Test
    void exactDigestVersionAndIdentityChecksFailClosedWithoutDeletion() {
        RetirementMetadataStoreTestSupport.FakeClient client = new RetirementMetadataStoreTestSupport.FakeClient();
        ObjectAuditRetirementStore store = new OxiaJavaObjectAuditRetirementStore(client);
        OxiaKeyspace keys = new OxiaKeyspace(CLUSTER);
        PartitionKey partition = keys.objectPartitionKey(OBJECT);
        String referencesKey = keys.objectReferencesKey(OBJECT);
        ObjectReferenceRecord references = RetirementMetadataStoreTestSupport.references(OBJECT_ID);
        client.put(referencesKey, partition, references, ObjectReferenceRecord.class, 91);

        assertThatThrownBy(() -> store.deleteReferences(
                        CLUSTER, OBJECT, 91, WRONG_DIGEST).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
        assertThatThrownBy(() -> store.deleteReferences(
                        CLUSTER,
                        OBJECT,
                        92,
                        RetirementMetadataStoreTestSupport.digest(
                                references, ObjectReferenceRecord.class)).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
        assertThat(client.contains(referencesKey, partition)).isTrue();

        ObjectReferenceRecord contradictory = RetirementMetadataStoreTestSupport.references("other-object");
        client.put(referencesKey, partition, contradictory, ObjectReferenceRecord.class, 93);
        assertThatThrownBy(() -> store.getReferences(CLUSTER, OBJECT).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(
                                        ErrorCode.METADATA_INVARIANT_VIOLATION)));
        assertThat(client.contains(referencesKey, partition)).isTrue();
    }

    @Test
    void absenceAndLostDeleteResponseRequireCoordinatorLevelReproof() {
        RetirementMetadataStoreTestSupport.FakeClient client = new RetirementMetadataStoreTestSupport.FakeClient();
        ObjectAuditRetirementStore store = new OxiaJavaObjectAuditRetirementStore(client);
        OxiaKeyspace keys = new OxiaKeyspace(CLUSTER);
        PartitionKey partition = keys.objectPartitionKey(OBJECT);
        String manifestKey = keys.objectManifestKey(OBJECT);
        ObjectManifestRecord manifest = RetirementMetadataStoreTestSupport.manifest(OBJECT_ID);
        client.put(manifestKey, partition, manifest, ObjectManifestRecord.class, 101);
        client.loseNextDeleteResponse();

        assertThatThrownBy(() -> store.deleteManifest(
                        CLUSTER,
                        OBJECT,
                        101,
                        RetirementMetadataStoreTestSupport.digest(
                                manifest, ObjectManifestRecord.class)).join())
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(client.contains(manifestKey, partition)).isFalse();
        assertThat(store.getManifest(CLUSTER, OBJECT).join()).isEmpty();
        assertThatThrownBy(() -> store.deleteManifest(
                        CLUSTER,
                        OBJECT,
                        101,
                        RetirementMetadataStoreTestSupport.digest(
                                manifest, ObjectManifestRecord.class)).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);

        store.close();
        assertThatThrownBy(() -> store.getManifest(CLUSTER, OBJECT))
                .isInstanceOfSatisfying(NereusException.class, nereus ->
                        assertThat(nereus.code()).isEqualTo(ErrorCode.STORAGE_CLOSED));
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
