/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

/** Replayable exact ciphertext Object and typed physical leaf identity. */
public final class Nwg1SealedObjectV1 {
    private final Nwg1HeaderV1 header;
    private final Nwg1DirectoryV1 directory;
    private final byte[] body;
    private final byte[] bodySha256;
    private final String leafUtf8;

    Nwg1SealedObjectV1(
            Nwg1HeaderV1 header, Nwg1DirectoryV1 directory, byte[] body, byte[] bodySha256, String leafUtf8) {
        this.header = header;
        this.directory = directory;
        this.body = body.clone();
        this.bodySha256 = bodySha256.clone();
        this.leafUtf8 = leafUtf8;
    }

    public Nwg1HeaderV1 header() {
        return header;
    }

    public Nwg1DirectoryV1 directory() {
        return directory;
    }

    public byte[] body() {
        return body.clone();
    }

    public byte[] bodySha256() {
        return bodySha256.clone();
    }

    public String leafUtf8() {
        return leafUtf8;
    }
}
