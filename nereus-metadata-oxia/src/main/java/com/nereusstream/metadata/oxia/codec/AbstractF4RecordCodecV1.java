/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

abstract class AbstractF4RecordCodecV1<T> implements MetadataRecordCodec<T> {
    static final int VERSION = 1;
    private final Class<T> recordClass;

    AbstractF4RecordCodecV1(Class<T> recordClass) {
        this.recordClass = recordClass;
    }

    @Override
    public final String recordType() {
        return recordClass.getSimpleName();
    }

    @Override
    public final int schemaVersion() {
        return VERSION;
    }

    @Override
    public final int minReaderSchemaVersion() {
        return VERSION;
    }

    final F4Binary.Reader reader(byte[] bytes) {
        F4Binary.Reader reader = new F4Binary.Reader(bytes);
        int version = reader.readUnsignedShort("schemaVersion");
        if (version != VERSION) {
            throw new MetadataCodecException("unsupported " + recordType() + " payload version: " + version);
        }
        return reader;
    }

    final F4Binary.Writer writer() {
        F4Binary.Writer writer = new F4Binary.Writer();
        writer.writeUnsignedShort(VERSION);
        return writer;
    }

    final MetadataCodecException malformed(Throwable failure) {
        return F4Binary.malformed(recordType(), failure);
    }
}
