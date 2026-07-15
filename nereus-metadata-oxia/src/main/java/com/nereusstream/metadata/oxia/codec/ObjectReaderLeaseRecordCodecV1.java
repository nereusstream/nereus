/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;

public final class ObjectReaderLeaseRecordCodecV1
        extends AbstractF4RecordCodecV1<ObjectReaderLeaseRecord> {
    public ObjectReaderLeaseRecordCodecV1() {
        super(ObjectReaderLeaseRecord.class);
    }

    @Override
    public byte[] encode(ObjectReaderLeaseRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.objectKeyHash());
            writer.writeString(value.processRunId());
            writer.writeString(value.leaseId());
            writer.writeLong(value.rootLifecycleEpoch());
            writer.writeLong(value.acquiredAtMillis());
            writer.writeLong(value.expiresAtMillis());
            writer.writeLong(value.maximumReadDeadlineMillis());
            writer.writeLong(value.renewalSequence());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public ObjectReaderLeaseRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            ObjectReaderLeaseRecord value = new ObjectReaderLeaseRecord(
                    VERSION,
                    reader.readString("objectKeyHash"),
                    reader.readString("processRunId"),
                    reader.readString("leaseId"),
                    reader.readLong("rootLifecycleEpoch"),
                    reader.readLong("acquiredAtMillis"),
                    reader.readLong("expiresAtMillis"),
                    reader.readLong("maximumReadDeadlineMillis"),
                    reader.readLong("renewalSequence"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
