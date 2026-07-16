/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.GcDomainSnapshotProofRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import java.util.ArrayList;
import java.util.List;

public final class GcRetirementManifestRecordCodecV1
        extends AbstractF4RecordCodecV1<GcRetirementManifestRecord> {
    public GcRetirementManifestRecordCodecV1() {
        super(GcRetirementManifestRecord.class);
    }

    @Override
    public byte[] encode(GcRetirementManifestRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.objectKeyHash());
            writer.writeString(value.gcAttemptId());
            writer.writeInt(value.referenceSetProtocolVersion());
            writer.writeString(value.queryIdentitySha256());
            writer.writeInt(value.domainProofs().size());
            for (GcDomainSnapshotProofRecord proof : value.domainProofs()) {
                writer.writeString(proof.domainId());
                writer.writeInt(proof.protocolVersion());
                writer.writeString(proof.queryIdentitySha256());
                writer.writeString(proof.snapshotSha256());
            }
            writer.writeInt(value.protectionCount());
            writer.writeInt(value.metadataRemovalCount());
            writer.writeString(value.referenceSetSha256());
            writer.writeLong(value.createdAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public GcRetirementManifestRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            String objectKeyHash = reader.readString("objectKeyHash");
            String gcAttemptId = reader.readString("gcAttemptId");
            int referenceSetProtocolVersion = reader.readInt("referenceSetProtocolVersion");
            String queryIdentitySha256 = reader.readString("queryIdentitySha256");
            int domainCount = reader.readCount(
                    "domainProofCount", Integer.BYTES * 2 + 128, 32);
            List<GcDomainSnapshotProofRecord> domainProofs = new ArrayList<>(domainCount);
            for (int index = 0; index < domainCount; index++) {
                domainProofs.add(new GcDomainSnapshotProofRecord(
                        reader.readString("domainId"),
                        reader.readInt("domainProtocolVersion"),
                        reader.readString("domainQueryIdentitySha256"),
                        reader.readString("domainSnapshotSha256")));
            }
            GcRetirementManifestRecord value = new GcRetirementManifestRecord(
                    VERSION,
                    objectKeyHash,
                    gcAttemptId,
                    referenceSetProtocolVersion,
                    queryIdentitySha256,
                    domainProofs,
                    reader.readInt("protectionCount"),
                    reader.readInt("metadataRemovalCount"),
                    reader.readString("referenceSetSha256"),
                    reader.readLong("createdAtMillis"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
