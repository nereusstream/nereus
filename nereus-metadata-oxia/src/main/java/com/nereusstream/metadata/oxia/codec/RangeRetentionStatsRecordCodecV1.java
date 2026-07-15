/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;

public final class RangeRetentionStatsRecordCodecV1
        extends AbstractF4RecordCodecV1<RangeRetentionStatsRecord> {
    public RangeRetentionStatsRecordCodecV1() {
        super(RangeRetentionStatsRecord.class);
    }

    @Override
    public byte[] encode(RangeRetentionStatsRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.streamId());
            writer.writeLong(value.offsetStart());
            writer.writeLong(value.offsetEnd());
            writer.writeLong(value.commitVersion());
            writer.writeLong(value.cumulativeSizeAtStart());
            writer.writeLong(value.cumulativeSizeAtEnd());
            writer.writeLong(value.minPublishTimeMillis());
            writer.writeLong(value.maxPublishTimeMillis());
            writer.writeString(value.sourceIndexKey());
            writer.writeString(value.sourceIndexIdentitySha256());
            writer.writeLong(value.sourceIndexMetadataVersion());
            writer.writeString(value.verifierBuild());
            writer.writeLong(value.verifiedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public RangeRetentionStatsRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            RangeRetentionStatsRecord value = new RangeRetentionStatsRecord(
                    VERSION,
                    reader.readString("streamId"),
                    reader.readLong("offsetStart"),
                    reader.readLong("offsetEnd"),
                    reader.readLong("commitVersion"),
                    reader.readLong("cumulativeSizeAtStart"),
                    reader.readLong("cumulativeSizeAtEnd"),
                    reader.readLong("minPublishTimeMillis"),
                    reader.readLong("maxPublishTimeMillis"),
                    reader.readString("sourceIndexKey"),
                    reader.readString("sourceIndexIdentitySha256"),
                    reader.readLong("sourceIndexMetadataVersion"),
                    reader.readString("verifierBuild"),
                    reader.readLong("verifiedAtMillis"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
