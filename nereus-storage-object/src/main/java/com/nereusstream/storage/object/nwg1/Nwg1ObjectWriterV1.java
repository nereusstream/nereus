/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/** Deterministic sealed-plan NWG1 writer. */
public final class Nwg1ObjectWriterV1 {
    private Nwg1ObjectWriterV1() {}

    public static Nwg1SealedObjectV1 seal(
            GroupEncodingPlanV1 plan,
            long laneSequence,
            byte[] walRunKey,
            Nwg1VerificationContextV1 verificationContext) {
        Nwg1SealedObjectV1 sealed = sealEncodedPlan(plan, laneSequence, walRunKey);
        Nwg1ObjectReaderV1.read(sealed.body(), sealed.bodySha256(), verificationContext, walRunKey);
        return sealed;
    }

    static Nwg1SealedObjectV1 sealEncodedPlan(GroupEncodingPlanV1 plan, long laneSequence, byte[] walRunKey) {
        int directoryLength = plan.predictedDirectoryLength();
        long directoryStored = Math.addExact(directoryLength, 16L);
        long prefixEnd = Math.addExact(256L, directoryStored);
        long bodyLength = plan.canonicalBodyLength(prefixEnd);
        Nwg1DirectoryV1 directory = plan.buildDirectory(prefixEnd);
        byte[] directoryPlain = Nwg1DirectoryCodecV1.encode(directory);
        if (directoryPlain.length != directoryLength) {
            throw new IllegalStateException("Directory plan changed");
        }
        Nwg1HeaderV1 header = new Nwg1HeaderV1(
                plan.protocolKind(),
                plan.shardId(),
                plan.shardRunEpoch(),
                laneSequence,
                plan.packingPolicyVersion(),
                plan.resolvedTargetBytes(),
                plan.resolvedLingerNanos(),
                plan.actualPayloadBytes(),
                plan.actualCloseLingerNanos(),
                directoryLength,
                directoryStored,
                plan.bindings().size(),
                plan.appendUnits().size(),
                plan.frames().size(),
                prefixEnd,
                bodyLength,
                plan.laneId(),
                plan.closeReason(),
                plan.protocolCellCommitment(),
                plan.providerScopeId(),
                plan.rootSha256(),
                plan.envelopeCommitment());
        byte[] exactHeader = Nwg1HeaderCodecV1.encode(header);
        byte[] info = Nwg1CryptoV1.objectKeyInfo(plan.shardId(), plan.shardRunEpoch(), plan.laneId(), laneSequence);
        byte[] objectKey = Nwg1CryptoV1.deriveObjectAeadKey(walRunKey, plan.rootSha256(), info);
        try {
            byte[] encryptedDirectory = Nwg1CryptoV1.encrypt(
                    objectKey, Nwg1CryptoV1.directoryNonce(), Nwg1CryptoV1.directoryAad(exactHeader), directoryPlain);
            ByteArrayOutputStream body = new ByteArrayOutputStream(Math.toIntExact(bodyLength));
            body.writeBytes(exactHeader);
            body.writeBytes(encryptedDirectory);
            for (int ordinal = 0; ordinal < plan.frames().size(); ordinal++) {
                byte[] row = Nwg1DirectoryCodecV1.frameRowBytes(directory, ordinal);
                byte[] encrypted = Nwg1CryptoV1.encrypt(
                        objectKey,
                        Nwg1CryptoV1.frameNonce(ordinal),
                        Nwg1CryptoV1.frameAad(exactHeader, ordinal, row),
                        plan.frames().get(ordinal).preAeadBytes());
                body.writeBytes(encrypted);
            }
            byte[] exactBody = body.toByteArray();
            if (exactBody.length != bodyLength) {
                throw new IllegalStateException("body length changed");
            }
            byte[] sha = Nwg1CommitmentsV1.sha256(exactBody);
            String leaf = String.format(
                    "%d/%019d/%019d-%019d-sha256-v1-%s.nwg",
                    plan.laneId(), laneSequence, prefixEnd, bodyLength, hex(sha));
            return new Nwg1SealedObjectV1(header, directory, exactBody, sha, leaf);
        } finally {
            Arrays.fill(objectKey, (byte) 0);
            Arrays.fill(info, (byte) 0);
        }
    }

    private static String hex(byte[] value) {
        return java.util.HexFormat.of().formatHex(value);
    }
}
