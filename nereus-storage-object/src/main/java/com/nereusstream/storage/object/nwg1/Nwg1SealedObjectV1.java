/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;

/** Replayable exact ciphertext Object and typed physical leaf identity. */
public final class Nwg1SealedObjectV1 {
    private final Nwg1HeaderV1 header;
    private final Nwg1DirectoryV1 directory;
    private final byte[] body;
    private final byte[] bodySha256;
    private final String leafUtf8;
    private SelfVerification selfVerification;

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

    public int bodyLength() {
        return body.length;
    }

    /** Read-only stream over this object's privately owned immutable ciphertext; no full-body copy. */
    public InputStream openBodyStream() {
        return new ByteArrayInputStream(body);
    }

    void recordSelfVerification(
            GroupEncodingPlanV1 plan, Nwg1VerificationContextV1 context, Nwg1ObjectReaderV1.DecodedObject decoded) {
        if (selfVerification != null) {
            throw new IllegalStateException("NWG1 self-verification is already recorded");
        }
        selfVerification = new SelfVerification(
                plan,
                context,
                new Nwg1ObjectReaderV1.AuthenticatedPrefix(
                        decoded.header(),
                        decoded.directory(),
                        Arrays.copyOf(body, Nwg1ConstantsV1.HEADER_BYTES),
                        body.length));
    }

    public Optional<Nwg1ObjectReaderV1.AuthenticatedPrefix> selfCheckedPrefix(Nwg1VerificationContextV1 context) {
        return selfVerification != null && selfVerification.context == context
                ? Optional.of(selfVerification.prefix)
                : Optional.empty();
    }

    GroupEncodingPlanV1 requireSelfCheckedPlan(Nwg1VerificationContextV1 context) {
        if (selfCheckedPrefix(context).isEmpty()) {
            throw new IllegalArgumentException("NWG1 self-verification does not belong to this exact context");
        }
        return selfVerification.plan;
    }

    private record SelfVerification(
            GroupEncodingPlanV1 plan,
            Nwg1VerificationContextV1 context,
            Nwg1ObjectReaderV1.AuthenticatedPrefix prefix) {}

    public byte[] bodySha256() {
        return bodySha256.clone();
    }

    public String leafUtf8() {
        return leafUtf8;
    }
}
