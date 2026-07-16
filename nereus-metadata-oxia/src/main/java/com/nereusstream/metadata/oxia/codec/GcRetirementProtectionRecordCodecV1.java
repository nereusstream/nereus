/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.GcRetirementProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;

public final class GcRetirementProtectionRecordCodecV1
        extends AbstractF4RecordCodecV1<GcRetirementProtectionRecord> {
    private final ObjectProtectionRecordCodecV1 protectionCodec =
            new ObjectProtectionRecordCodecV1();

    public GcRetirementProtectionRecordCodecV1() {
        super(GcRetirementProtectionRecord.class);
    }

    @Override
    public byte[] encode(GcRetirementProtectionRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.objectKeyHash());
            writer.writeString(value.gcAttemptId());
            writer.writeString(value.protectionKey());
            writer.writeLong(value.protectionMetadataVersion());
            writer.writeString(value.protectionDurableValueSha256());
            writer.writeBytes(protectionCodec.encode(value.protection()));
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public GcRetirementProtectionRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            String objectKeyHash = reader.readString("objectKeyHash");
            String gcAttemptId = reader.readString("gcAttemptId");
            String protectionKey = reader.readString("protectionKey");
            long protectionMetadataVersion = reader.readLong("protectionMetadataVersion");
            String protectionDurableValueSha256 = reader.readString(
                    "protectionDurableValueSha256");
            ObjectProtectionRecord protection = protectionCodec.decode(
                    reader.readBytes("protection"));
            GcRetirementProtectionRecord value = new GcRetirementProtectionRecord(
                    VERSION,
                    objectKeyHash,
                    gcAttemptId,
                    protectionKey,
                    protectionMetadataVersion,
                    protectionDurableValueSha256,
                    protection,
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
