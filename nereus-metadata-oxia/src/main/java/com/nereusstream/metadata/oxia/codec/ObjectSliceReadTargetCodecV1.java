/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import java.util.Optional;

/** NRO1 object-slice target payload. */
public final class ObjectSliceReadTargetCodecV1 implements ReadTargetCodec<ObjectSliceReadTarget> {
    @Override public ReadTargetType targetType() { return ReadTargetType.OBJECT_SLICE; }
    @Override public int targetVersion() { return 1; }
    @Override public Class<ObjectSliceReadTarget> targetClass() { return ObjectSliceReadTarget.class; }

    @Override
    public byte[] encode(ObjectSliceReadTarget target) {
        if (target.version() != 1) throw new MetadataCodecException("unsupported object target version");
        CanonicalTargetBinary.Writer writer = new CanonicalTargetBinary.Writer();
        writer.magic("NRO1");
        writer.string(target.objectId().value());
        writer.string(target.objectKey().value());
        writer.string(target.objectType().name());
        writer.string(target.physicalFormat());
        writer.string(target.logicalFormat());
        writer.string(target.sliceId());
        writer.longValue(target.objectOffset());
        writer.longValue(target.objectLength());
        writer.string(target.sliceChecksum().type().name());
        writer.string(target.sliceChecksum().value());
        EntryIndexRef index = target.entryIndexRef();
        writer.string(index.location().name());
        writer.string(index.objectId().map(ObjectId::value).orElse(""));
        writer.string(index.objectKey().map(ObjectKey::value).orElse(""));
        writer.byteArray(index.inlineData().orElseGet(() -> new byte[0]));
        writer.longValue(index.offset());
        writer.longValue(index.length());
        writer.string(index.checksum().type().name());
        writer.string(index.checksum().value());
        return writer.finish();
    }

    @Override
    public ObjectSliceReadTarget decode(byte[] payload) {
        try {
            CanonicalTargetBinary.Reader reader = new CanonicalTargetBinary.Reader(payload);
            reader.magic("NRO1");
            ObjectId objectId = new ObjectId(reader.string("objectId"));
            ObjectKey objectKey = new ObjectKey(reader.string("objectKey"));
            ObjectType objectType = ObjectType.valueOf(reader.string("objectType"));
            String physicalFormat = reader.string("physicalFormat");
            String logicalFormat = reader.string("logicalFormat");
            String sliceId = reader.string("sliceId");
            long objectOffset = reader.longValue();
            long objectLength = reader.longValue();
            Checksum sliceChecksum = checksum(reader);
            EntryIndexLocation location = EntryIndexLocation.valueOf(reader.string("entryIndex.location"));
            String indexObjectId = reader.string("entryIndex.objectId");
            String indexObjectKey = reader.string("entryIndex.objectKey");
            byte[] inline = reader.byteArray();
            long indexOffset = reader.longValue();
            long indexLength = reader.longValue();
            Checksum indexChecksum = checksum(reader);
            reader.finish();
            EntryIndexRef index = new EntryIndexRef(
                    location,
                    indexObjectId.isEmpty() ? Optional.empty() : Optional.of(new ObjectId(indexObjectId)),
                    indexObjectKey.isEmpty() ? Optional.empty() : Optional.of(new ObjectKey(indexObjectKey)),
                    inline.length == 0 ? Optional.empty() : Optional.of(inline),
                    indexOffset, indexLength, indexChecksum);
            return new ObjectSliceReadTarget(1, objectId, objectKey, objectType,
                    physicalFormat, logicalFormat, sliceId, objectOffset, objectLength, sliceChecksum, index);
        } catch (MetadataCodecException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new MetadataCodecException("malformed object target payload", e);
        }
    }

    private static Checksum checksum(CanonicalTargetBinary.Reader reader) {
        return new Checksum(ChecksumType.valueOf(reader.string("checksumType")), reader.string("checksumValue"));
    }
}
