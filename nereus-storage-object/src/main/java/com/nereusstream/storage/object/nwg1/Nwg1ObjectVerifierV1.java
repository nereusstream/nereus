/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Fixed-order production entry point from preloaded Root authority and acquired Object bytes. */
public final class Nwg1ObjectVerifierV1 {
    private static final Pattern LEAF =
            Pattern.compile("([0-2])/([0-9]{19})/([0-9]{19})-([0-9]{19})-sha256-v1-([0-9a-f]{64})\\.nwg");

    @FunctionalInterface
    public interface RunKeyUnwrapper {
        /** Returns one newly owned 32-byte plaintext WalRun key. */
        byte[] unwrap(Nwg1EnvelopeV1 envelope);
    }

    public record Request(
            Nwg1VerificationPathV1 path,
            Nwg1RootAuthorityV1 rootAuthority,
            Nwg1VerificationContextV1 verificationContext,
            byte[] leafUtf8,
            byte[] canonicalBody,
            long selectedFrameOrdinal,
            RunKeyUnwrapper runKeyUnwrapper) {
        public Request {
            path = Objects.requireNonNull(path, "path");
            rootAuthority = Objects.requireNonNull(rootAuthority, "rootAuthority");
            verificationContext = Objects.requireNonNull(verificationContext, "verificationContext");
            leafUtf8 = Objects.requireNonNull(leafUtf8, "leafUtf8").clone();
            canonicalBody =
                    Objects.requireNonNull(canonicalBody, "canonicalBody").clone();
            if (selectedFrameOrdinal < 0) {
                throw new IllegalArgumentException("selectedFrameOrdinal must be non-negative");
            }
            runKeyUnwrapper = Objects.requireNonNull(runKeyUnwrapper, "runKeyUnwrapper");
        }

        @Override
        public byte[] leafUtf8() {
            return leafUtf8.clone();
        }

        @Override
        public byte[] canonicalBody() {
            return canonicalBody.clone();
        }
    }

    private record LeafFacts(int laneId, long laneSequence, long prefixEnd, long bodyLength, byte[] bodySha256) {
        private LeafFacts {
            bodySha256 = bodySha256.clone();
        }
    }

    private Nwg1ObjectVerifierV1() {}

    public static Nwg1ObjectReaderV1.DecodedObject verify(Request request) {
        Objects.requireNonNull(request, "request");
        verifyRootAuthority(request.rootAuthority(), request.verificationContext());
        LeafFacts leaf = parseLeaf(request.leafUtf8());
        byte[] body = request.canonicalBody();
        if (request.path() != Nwg1VerificationPathV1.ROUTINE_RANGE_READ
                && !MessageDigest.isEqual(leaf.bodySha256(), Nwg1CommitmentsV1.sha256(body))) {
            fail(
                    Nwg1RejectionV1.DIGEST_MISMATCH,
                    Nwg1ValidationStageV1.OBJECT_BODY_DIGEST,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    "Object body digest mismatch");
        }
        if (body.length < Nwg1ConstantsV1.HEADER_BYTES) {
            fail(
                    Nwg1RejectionV1.TRUNCATED_INPUT,
                    Nwg1ValidationStageV1.HEADER_GRAMMAR,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    "short Object body");
        }
        Nwg1HeaderV1 header = Nwg1HeaderCodecV1.decode(Arrays.copyOf(body, Nwg1ConstantsV1.HEADER_BYTES));
        verifyHeaderAuthority(header, leaf, request.rootAuthority(), request.verificationContext(), body.length);

        Nwg1EnvelopeV1 envelope = Nwg1EnvelopeV1.decode(request.rootAuthority().framedEnvelope());
        if (!MessageDigest.isEqual(
                request.rootAuthority().wrappedEnvelopeCommitment(), Nwg1CommitmentsV1.wrappedEnvelope(envelope))) {
            fail(
                    Nwg1RejectionV1.AUTHORITY_MISMATCH,
                    Nwg1ValidationStageV1.KMS_ENVELOPE,
                    Nwg1IsolationScopeV1.WALRUN,
                    "Root envelope commitment mismatch");
        }
        byte[] runKey;
        try {
            runKey = request.runKeyUnwrapper().unwrap(envelope);
        } catch (Nwg1ValidationException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.KEY_UNWRAP_FAILED,
                    Nwg1ValidationStageV1.KMS_ENVELOPE,
                    Nwg1IsolationScopeV1.WALRUN,
                    "KMS unwrap failed",
                    e);
        }
        if (runKey == null || runKey.length != 32) {
            if (runKey != null) {
                Arrays.fill(runKey, (byte) 0);
            }
            fail(
                    Nwg1RejectionV1.KEY_UNWRAP_FAILED,
                    Nwg1ValidationStageV1.KMS_ENVELOPE,
                    Nwg1IsolationScopeV1.WALRUN,
                    "KMS unwrap did not return 32 bytes");
        }
        try {
            if (request.path() == Nwg1VerificationPathV1.ROUTINE_RANGE_READ) {
                return Nwg1ObjectReaderV1.readRoutineFrame(
                        body, request.verificationContext(), runKey, Math.toIntExact(request.selectedFrameOrdinal()));
            }
            return Nwg1ObjectReaderV1.readVerifiedBody(
                    body, leaf.bodySha256(), request.verificationContext(), runKey, false);
        } finally {
            Arrays.fill(runKey, (byte) 0);
        }
    }

    private static void verifyRootAuthority(Nwg1RootAuthorityV1 root, Nwg1VerificationContextV1 verificationContext) {
        if (!MessageDigest.isEqual(root.exactNpc1(), verificationContext.exactNpc1())
                || !MessageDigest.isEqual(
                        root.protocolCellCommitment(), Nwg1CommitmentsV1.protocolCell(root.exactNpc1()))
                || !MessageDigest.isEqual(root.cellProviderScopeId(), verificationContext.cellProviderScopeId())
                || !MessageDigest.isEqual(root.walRunRootSha256(), verificationContext.walRunRootSha256())) {
            fail(
                    Nwg1RejectionV1.AUTHORITY_MISMATCH,
                    Nwg1ValidationStageV1.ROOT_AUTHORITY,
                    Nwg1IsolationScopeV1.WALRUN,
                    "injected Root authority mismatch");
        }
    }

    private static LeafFacts parseLeaf(byte[] leafUtf8) {
        String leaf;
        try {
            leaf = new String(leafUtf8, StandardCharsets.UTF_8);
            if (!Arrays.equals(leafUtf8, leaf.getBytes(StandardCharsets.UTF_8))) {
                throw new IllegalArgumentException("invalid UTF-8");
            }
        } catch (IllegalArgumentException e) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.NON_CANONICAL_ENCODING,
                    Nwg1ValidationStageV1.LEAF,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    "invalid leaf UTF-8",
                    e);
        }
        Matcher matcher = LEAF.matcher(leaf);
        if (!matcher.matches()) {
            fail(
                    Nwg1RejectionV1.NON_CANONICAL_ENCODING,
                    Nwg1ValidationStageV1.LEAF,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    "non-canonical NWG1 leaf");
        }
        try {
            long laneSequence = Long.parseLong(matcher.group(2));
            long prefixEnd = Long.parseLong(matcher.group(3));
            long bodyLength = Long.parseLong(matcher.group(4));
            if (prefixEnd < Nwg1ConstantsV1.HEADER_BYTES || bodyLength < prefixEnd) {
                fail(
                        Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                        Nwg1ValidationStageV1.LEAF,
                        Nwg1IsolationScopeV1.SHARED_OBJECT,
                        "invalid leaf range");
            }
            return new LeafFacts(
                    Integer.parseInt(matcher.group(1)),
                    laneSequence,
                    prefixEnd,
                    bodyLength,
                    HexFormat.of().parseHex(matcher.group(5)));
        } catch (NumberFormatException e) {
            throw new Nwg1ValidationException(
                    Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                    Nwg1ValidationStageV1.LEAF,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    "leaf number outside signed-long domain",
                    e);
        }
    }

    private static void verifyHeaderAuthority(
            Nwg1HeaderV1 header,
            LeafFacts leaf,
            Nwg1RootAuthorityV1 root,
            Nwg1VerificationContextV1 context,
            int actualBodyLength) {
        if (header.laneId() != leaf.laneId()
                || header.laneSequence() != leaf.laneSequence()
                || header.directoryPrefixEnd() != leaf.prefixEnd()
                || header.canonicalBodyLength() != leaf.bodyLength()
                || header.canonicalBodyLength() != actualBodyLength
                || header.protocolKind()
                        != context.protocolCell().protocolKind().code()
                || !MessageDigest.isEqual(header.protocolCellCommitment(), root.protocolCellCommitment())
                || !MessageDigest.isEqual(header.cellProviderScopeId(), root.cellProviderScopeId())
                || !MessageDigest.isEqual(header.walRunRootSha256(), root.walRunRootSha256())
                || !MessageDigest.isEqual(header.wrappedEnvelopeCommitment(), root.wrappedEnvelopeCommitment())) {
            fail(
                    Nwg1RejectionV1.AUTHORITY_MISMATCH,
                    Nwg1ValidationStageV1.HEADER_AUTHORITY,
                    Nwg1IsolationScopeV1.SHARED_OBJECT,
                    "Header/leaf/Root authority mismatch");
        }
    }

    private static void fail(
            Nwg1RejectionV1 rejection, Nwg1ValidationStageV1 stage, Nwg1IsolationScopeV1 scope, String message) {
        throw new Nwg1ValidationException(rejection, stage, scope, message);
    }
}
