/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Real byte-mutation/deep-resign runner used by the closed NWG1 negative corpus. */
final class Nwg1MutationRunnerV1 {
    static final List<String> EXTERNAL_CALL_KINDS = List.of(
            "ROOT_AUTHORITY_READ",
            "METADATA_READ",
            "METADATA_CONDITIONAL_MUTATION",
            "KMS_WRAP",
            "KMS_UNWRAP",
            "OBJECT_CONDITIONAL_PUT",
            "OBJECT_HEAD",
            "OBJECT_FULL_GET",
            "OBJECT_PREFIX_RANGE_GET",
            "OBJECT_FRAME_RANGE_GET",
            "OBJECT_LIST_PAGE");

    record Operation(
            String componentKind, int rowOrdinal, Nwg1MutationOperationV1 operation, int offset, byte[] operand) {
        Operation {
            operand = operand.clone();
        }

        @Override
        public byte[] operand() {
            return operand.clone();
        }
    }

    record Spec(
            String mutationId,
            List<Operation> operations,
            Set<Nwg1ResignOperationV1> resignOperations,
            byte[] mutationRoot) {
        Spec {
            operations = List.copyOf(operations);
            resignOperations = Set.copyOf(resignOperations);
            mutationRoot = mutationRoot == null ? null : mutationRoot.clone();
        }

        @Override
        public byte[] mutationRoot() {
            return mutationRoot == null ? null : mutationRoot.clone();
        }
    }

    record Execution(
            Nwg1ValidationException failure,
            String publication,
            Map<String, Integer> externalCalls,
            byte[] derivedObjectKeySha256,
            int changedComponents) {
        Execution {
            externalCalls = Map.copyOf(externalCalls);
            derivedObjectKeySha256 = derivedObjectKeySha256.clone();
        }

        @Override
        public byte[] derivedObjectKeySha256() {
            return derivedObjectKeySha256.clone();
        }
    }

    private Nwg1MutationRunnerV1() {}

    static Execution execute(Nwg1GoldenCorpusV1.Vector vector, Spec spec, Nwg1VerificationPathV1 verificationPath) {
        State state = new State(vector);
        if (spec.mutationRoot() != null) {
            state.applyMutationRoot(spec.mutationRoot());
        }
        int changed = 0;
        for (Operation operation : spec.operations()) {
            changed += state.apply(operation);
        }
        try {
            state.resign(spec.resignOperations());
        } catch (RuntimeException failure) {
            throw new AssertionError("mutation resign failed: " + spec.mutationId(), failure);
        }
        Map<String, Integer> calls = zeroExternalCalls();
        Nwg1ValidationException failure;
        try {
            Nwg1ObjectVerifierV1.verify(new Nwg1ObjectVerifierV1.Request(
                    verificationPath,
                    state.rootAuthority(),
                    state.verificationContext(),
                    state.leaf,
                    state.body(),
                    0,
                    envelope -> {
                        calls.compute("KMS_UNWRAP", (ignored, count) -> count + 1);
                        if (!Arrays.equals(envelope.framedBytes(), state.baseEnvelope)) {
                            throw new IllegalStateException("synthetic KMS rejects changed wrapped key");
                        }
                        return state.walRunKey();
                    }));
            throw new AssertionError("mutation was accepted: " + spec.mutationId());
        } catch (Nwg1ValidationException expected) {
            failure = expected;
        }
        return new Execution(failure, "NONE", calls, Nwg1CommitmentsV1.sha256(state.canonicalObjectKey()), changed);
    }

    static byte[] apply(byte[] input, Nwg1MutationOperationV1 operation, int offset, byte[] operand) {
        byte[] result = input.clone();
        return switch (operation) {
            case SET_U16 -> set(result, offset, operand, 2);
            case SET_U32 -> set(result, offset, operand, 4);
            case SET_U64 -> set(result, offset, operand, 8);
            case XOR_BYTE -> {
                requireRange(result, offset, 1);
                if (operand.length != 1) {
                    throw new IllegalArgumentException("XOR_BYTE operand width");
                }
                result[offset] ^= operand[0];
                yield result;
            }
            case REPLACE_COMPONENT -> operand.clone();
            case TRUNCATE_COMPONENT -> Arrays.copyOf(result, offset);
            case APPEND_BYTES -> {
                byte[] joined = Arrays.copyOf(result, result.length + operand.length);
                System.arraycopy(operand, 0, joined, result.length, operand.length);
                yield joined;
            }
            case SWAP_ROWS -> swapRows(result, offset, exactInt(operand), 48);
            case DUPLICATE_ROW -> duplicateRow(result, offset, 48);
            case REMOVE_ROW -> removeRow(result, offset, 48);
        };
    }

    static byte[] mutationRoot(byte[] baseRoot, String mutationId, byte[] recipeSha256) {
        byte[] id = mutationId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer preimage = ByteBuffer.allocate(Nwg1ConstantsV1.MUTATION_ROOT_DOMAIN.length + 32 + 4 + id.length + 32)
                .order(ByteOrder.BIG_ENDIAN)
                .put(Nwg1ConstantsV1.MUTATION_ROOT_DOMAIN)
                .put(baseRoot)
                .putInt(id.length)
                .put(id)
                .put(recipeSha256);
        return Nwg1CommitmentsV1.sha256(preimage.array());
    }

    private static Map<String, Integer> zeroExternalCalls() {
        Map<String, Integer> result = new LinkedHashMap<>();
        for (String kind : EXTERNAL_CALL_KINDS) {
            result.put(kind, 0);
        }
        return result;
    }

    private static final class State {
        private final byte[] baseEnvelope;
        private final byte[] walRunKey;
        private final int leafLaneId;
        private final long leafLaneSequence;
        private final long leafPrefixEnd;
        private final int frameRowStart;
        private byte[] rootProtocolCommitment;
        private byte[] rootEnvelopeCommitment;
        private byte[] rootWalRun;
        private byte[] framedEnvelope;
        private byte[] header;
        private byte[] directoryPlain;
        private byte[] directoryCipher;
        private final List<byte[]> framePlain = new ArrayList<>();
        private final List<byte[]> frameCipher = new ArrayList<>();
        private byte[] info;
        private byte[] objectKey;
        private byte[] directoryNonce;
        private byte[] directoryAad;
        private final List<byte[]> frameNonce = new ArrayList<>();
        private final List<byte[]> frameAad = new ArrayList<>();
        private byte[] leaf;
        private byte[] bodyOverride;
        private boolean explicitObjectKeyMutation;
        private boolean explicitDirectoryAadMutation;
        private final Set<Integer> explicitFrameAadOrdinals = new java.util.HashSet<>();
        private Nwg1VerificationContextV1 verificationContext;

        private State(Nwg1GoldenCorpusV1.Vector vector) {
            Nwg1SealedObjectV1 sealed = vector.sealed();
            walRunKey = vector.walRunKey();
            Nwg1HeaderV1 decodedHeader = sealed.header();
            verificationContext = vector.verificationContext();
            baseEnvelope = verificationContext.envelope().framedBytes();
            leafLaneId = decodedHeader.laneId();
            leafLaneSequence = decodedHeader.laneSequence();
            leafPrefixEnd = decodedHeader.directoryPrefixEnd();
            frameRowStart = 32
                    + Math.multiplyExact(sealed.directory().bindings().size(), Nwg1ConstantsV1.BINDING_ROW_BYTES)
                    + Math.multiplyExact(
                            sealed.directory().appendUnits().size(),
                            sealed.directory().protocolKind() == 2
                                    ? Nwg1ConstantsV1.PULSAR_APPEND_UNIT_ROW_BYTES
                                    : Nwg1ConstantsV1.KAFKA_APPEND_UNIT_ROW_BYTES);
            framedEnvelope = baseEnvelope.clone();
            rootProtocolCommitment = decodedHeader.protocolCellCommitment();
            rootEnvelopeCommitment = decodedHeader.wrappedEnvelopeCommitment();
            rootWalRun = decodedHeader.walRunRootSha256();
            header = Arrays.copyOf(sealed.body(), Nwg1ConstantsV1.HEADER_BYTES);
            info = Nwg1CryptoV1.objectKeyInfo(
                    decodedHeader.shardId(),
                    decodedHeader.shardRunEpoch(),
                    decodedHeader.laneId(),
                    decodedHeader.laneSequence());
            objectKey = Nwg1CryptoV1.deriveObjectAeadKey(walRunKey, rootWalRun, info);
            directoryNonce = Nwg1CryptoV1.directoryNonce();
            directoryAad = Nwg1CryptoV1.directoryAad(header);
            byte[] baseBody = sealed.body();
            int prefixEnd = Math.toIntExact(decodedHeader.directoryPrefixEnd());
            directoryCipher = Arrays.copyOfRange(baseBody, Nwg1ConstantsV1.HEADER_BYTES, prefixEnd);
            directoryPlain = Nwg1CryptoV1.decrypt(
                    objectKey, directoryNonce, directoryAad, directoryCipher, Nwg1ValidationStageV1.DIRECTORY_AEAD);
            for (int ordinal = 0; ordinal < sealed.directory().frames().size(); ordinal++) {
                Nwg1DirectoryV1.Frame row = sealed.directory().frames().get(ordinal);
                byte[] nonce = Nwg1CryptoV1.frameNonce(ordinal);
                byte[] aad = Nwg1CryptoV1.frameAad(
                        header, ordinal, Nwg1DirectoryCodecV1.frameRowBytes(sealed.directory(), ordinal));
                byte[] ciphertext = Arrays.copyOfRange(
                        baseBody,
                        Math.toIntExact(row.storedBodyOffset()),
                        Math.toIntExact(row.storedBodyOffset() + row.storedBlockBytes()));
                frameNonce.add(nonce);
                frameAad.add(aad);
                frameCipher.add(ciphertext);
                framePlain.add(
                        Nwg1CryptoV1.decrypt(objectKey, nonce, aad, ciphertext, Nwg1ValidationStageV1.FRAME_AEAD));
            }
            leaf = sealed.leafUtf8().getBytes(StandardCharsets.UTF_8);
        }

        private void applyMutationRoot(byte[] newRoot) {
            rootWalRun = newRoot.clone();
            System.arraycopy(rootWalRun, 0, header, 188, 32);
            verificationContext = new Nwg1VerificationContextV1(
                    verificationContext.protocolCell(),
                    verificationContext.cellProviderScopeId(),
                    rootWalRun,
                    verificationContext.envelope(),
                    verificationContext.ownerWitnessProvider(),
                    verificationContext.nativePayloadVerifier(),
                    0,
                    verificationContext.protocolCell().protocolKind().code() == 2 ? 1L << 40 : 0);
            objectKey = Nwg1CryptoV1.deriveObjectAeadKey(walRunKey, rootWalRun, info);
        }

        private int apply(Operation operation) {
            byte[] before = component(operation.componentKind(), operation.rowOrdinal());
            byte[] after =
                    Nwg1MutationRunnerV1.apply(before, operation.operation(), operation.offset(), operation.operand());
            if (Arrays.equals(before, after)) {
                throw new AssertionError("operation did not change component: " + operation);
            }
            replaceComponent(operation.componentKind(), operation.rowOrdinal(), after);
            return 1;
        }

        private byte[] component(String kind, int ordinal) {
            return switch (kind) {
                case "PROTOCOL_CELL_COMMITMENT" -> rootProtocolCommitment;
                // The manifest authors RKE_ENVELOPE_PREIMAGE_V1 for this kind. The
                // commitment itself changes only through the explicit re-sign token.
                case "WRAPPED_ENVELOPE_COMMITMENT" -> framedEnvelope;
                case "OWNER_FENCE_COMMITMENT" ->
                    Arrays.copyOfRange(directoryPlain, bindingStart(ordinal) + 64, bindingStart(ordinal) + 96);
                case "HEADER" -> header;
                case "HKDF_INFO" -> info;
                case "OBJECT_AEAD_KEY" -> objectKey;
                case "DIRECTORY_NONCE" -> directoryNonce;
                case "DIRECTORY_PLAINTEXT" -> directoryPlain;
                case "DIRECTORY_AAD" -> directoryAad;
                case "DIRECTORY_CIPHERTEXT_AND_TAG" -> directoryCipher;
                case "FRAME_PRE_AEAD" -> framePlain.get(ordinal);
                case "FRAME_NONCE" -> frameNonce.get(ordinal);
                case "FRAME_AAD" -> frameAad.get(ordinal);
                case "FRAME_CIPHERTEXT_AND_TAG" -> frameCipher.get(ordinal);
                case "CANONICAL_BODY" -> body();
                case "LEAF_UTF8" -> leaf;
                default -> throw new IllegalArgumentException("unknown component " + kind);
            };
        }

        private void replaceComponent(String kind, int ordinal, byte[] value) {
            switch (kind) {
                case "PROTOCOL_CELL_COMMITMENT" -> rootProtocolCommitment = value;
                case "WRAPPED_ENVELOPE_COMMITMENT" -> framedEnvelope = value;
                case "OWNER_FENCE_COMMITMENT" ->
                    System.arraycopy(value, 0, directoryPlain, bindingStart(ordinal) + 64, value.length);
                case "HEADER" -> header = value;
                case "HKDF_INFO" -> info = value;
                case "OBJECT_AEAD_KEY" -> {
                    objectKey = value;
                    explicitObjectKeyMutation = true;
                }
                case "DIRECTORY_NONCE" -> directoryNonce = value;
                case "DIRECTORY_PLAINTEXT" -> directoryPlain = value;
                case "DIRECTORY_AAD" -> {
                    directoryAad = value;
                    explicitDirectoryAadMutation = true;
                }
                case "DIRECTORY_CIPHERTEXT_AND_TAG" -> directoryCipher = value;
                case "FRAME_PRE_AEAD" -> framePlain.set(ordinal, value);
                case "FRAME_NONCE" -> frameNonce.set(ordinal, value);
                case "FRAME_AAD" -> {
                    frameAad.set(ordinal, value);
                    explicitFrameAadOrdinals.add(ordinal);
                }
                case "FRAME_CIPHERTEXT_AND_TAG" -> frameCipher.set(ordinal, value);
                case "CANONICAL_BODY" -> bodyOverride = value;
                case "LEAF_UTF8" -> leaf = value;
                default -> throw new IllegalArgumentException("unknown component " + kind);
            }
        }

        private void resign(Set<Nwg1ResignOperationV1> operations) {
            EnumSet<Nwg1ResignOperationV1> selected =
                    operations.isEmpty() ? EnumSet.noneOf(Nwg1ResignOperationV1.class) : EnumSet.copyOf(operations);
            if (selected.contains(Nwg1ResignOperationV1.RECOMPUTE_PROTOCOL_CELL_COMMITMENT)) {
                rootProtocolCommitment = Nwg1CommitmentsV1.protocolCell(verificationContext.exactNpc1());
                System.arraycopy(rootProtocolCommitment, 0, header, 124, 32);
            }
            if (selected.contains(Nwg1ResignOperationV1.RECOMPUTE_ENVELOPE_COMMITMENT)) {
                rootEnvelopeCommitment = Nwg1CommitmentsV1.wrappedEnvelope(Nwg1EnvelopeV1.decode(framedEnvelope));
                System.arraycopy(rootEnvelopeCommitment, 0, header, 220, 32);
            }
            if (selected.contains(Nwg1ResignOperationV1.RECOMPUTE_OWNER_FENCE_COMMITMENT)) {
                recomputeOwnerFences();
            }
            if (selected.contains(Nwg1ResignOperationV1.RECOMPUTE_DIRECTORY_CRC)) {
                recomputeDirectoryCrc();
            }
            if (selected.contains(Nwg1ResignOperationV1.RECOMPUTE_HEADER_CRC)) {
                recomputeHeaderCrc();
            }
            if (!explicitObjectKeyMutation
                    && (selected.contains(Nwg1ResignOperationV1.REENCRYPT_DIRECTORY)
                            || selected.contains(Nwg1ResignOperationV1.REENCRYPT_FRAME))) {
                objectKey = Nwg1CryptoV1.deriveObjectAeadKey(walRunKey, rootWalRun, info);
            }
            if (selected.contains(Nwg1ResignOperationV1.REENCRYPT_DIRECTORY)) {
                if (!explicitDirectoryAadMutation) {
                    directoryAad = Nwg1CryptoV1.directoryAad(header);
                }
                reencryptDirectory();
            }
            if (selected.contains(Nwg1ResignOperationV1.REENCRYPT_FRAME)) {
                refreshFrameAad();
                reencryptFrames();
            }
            if (selected.contains(Nwg1ResignOperationV1.RECOMPUTE_BODY_SHA_AND_LEAF)) {
                recomputeLeaf();
            }
        }

        private void recomputeOwnerFences() {
            int count = intAt(directoryPlain, 12);
            for (int ordinal = 0; ordinal < count; ordinal++) {
                int start = bindingStart(ordinal);
                byte[] bindingId = Arrays.copyOfRange(directoryPlain, start, start + 32);
                int kind = Short.toUnsignedInt(ByteBuffer.wrap(directoryPlain, start + 104, 2)
                        .order(ByteOrder.BIG_ENDIAN)
                        .getShort());
                int version = Short.toUnsignedInt(ByteBuffer.wrap(directoryPlain, start + 106, 2)
                        .order(ByteOrder.BIG_ENDIAN)
                        .getShort());
                byte[] witness = verificationContext.ownerWitnessProvider().canonicalWitness(bindingId, kind, version);
                byte[] commitment = Nwg1CommitmentsV1.ownerFence(kind, version, witness);
                System.arraycopy(commitment, 0, directoryPlain, start + 64, 32);
            }
        }

        private int bindingStart(int ordinal) {
            return 32 + Math.multiplyExact(ordinal, Nwg1ConstantsV1.BINDING_ROW_BYTES);
        }

        private void recomputeHeaderCrc() {
            if (header.length == Nwg1ConstantsV1.HEADER_BYTES) {
                ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN).putInt(252, Nwg1HeaderCodecV1.crc32c(header));
            }
        }

        private void recomputeDirectoryCrc() {
            if (directoryPlain.length >= 4) {
                ByteBuffer.wrap(directoryPlain)
                        .order(ByteOrder.BIG_ENDIAN)
                        .putInt(directoryPlain.length - 4, Nwg1DirectoryCodecV1.crc32c(directoryPlain));
            }
        }

        private void reencryptDirectory() {
            directoryCipher = Nwg1CryptoV1.encrypt(objectKey, directoryNonce, directoryAad, directoryPlain);
        }

        private void reencryptFrames() {
            for (int ordinal = 0; ordinal < framePlain.size(); ordinal++) {
                frameCipher.set(
                        ordinal,
                        Nwg1CryptoV1.encrypt(
                                objectKey, frameNonce.get(ordinal), frameAad.get(ordinal), framePlain.get(ordinal)));
            }
        }

        private void refreshFrameAad() {
            for (int ordinal = 0; ordinal < frameAad.size(); ordinal++) {
                if (explicitFrameAadOrdinals.contains(ordinal)) {
                    continue;
                }
                byte[] row = Arrays.copyOfRange(
                        directoryPlain,
                        frameRowStart + ordinal * Nwg1ConstantsV1.FRAME_ROW_BYTES,
                        frameRowStart + (ordinal + 1) * Nwg1ConstantsV1.FRAME_ROW_BYTES);
                frameAad.set(ordinal, Nwg1CryptoV1.frameAad(header, ordinal, row));
            }
        }

        private int intAt(byte[] source, int offset) {
            return ByteBuffer.wrap(source, offset, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
        }

        private void recomputeLeaf() {
            byte[] body = body();
            leaf = String.format(
                            "%d/%019d/%019d-%019d-sha256-v1-%s.nwg",
                            leafLaneId,
                            leafLaneSequence,
                            leafPrefixEnd,
                            body.length,
                            HexFormat.of().formatHex(Nwg1CommitmentsV1.sha256(body)))
                    .getBytes(StandardCharsets.UTF_8);
        }

        private byte[] body() {
            if (bodyOverride != null) {
                return bodyOverride.clone();
            }
            int length = header.length + directoryCipher.length;
            for (byte[] frame : frameCipher) {
                length = Math.addExact(length, frame.length);
            }
            ByteBuffer result = ByteBuffer.allocate(length);
            result.put(header).put(directoryCipher);
            for (byte[] frame : frameCipher) {
                result.put(frame);
            }
            return result.array();
        }

        private byte[] canonicalObjectKey() {
            return Nwg1CryptoV1.deriveObjectAeadKey(walRunKey, rootWalRun, info);
        }

        private byte[] walRunKey() {
            return walRunKey.clone();
        }

        private Nwg1RootAuthorityV1 rootAuthority() {
            return new Nwg1RootAuthorityV1(
                    verificationContext.exactNpc1(),
                    rootProtocolCommitment,
                    verificationContext.cellProviderScopeId(),
                    rootWalRun,
                    framedEnvelope,
                    rootEnvelopeCommitment);
        }

        private Nwg1VerificationContextV1 verificationContext() {
            return verificationContext;
        }
    }

    private static byte[] set(byte[] target, int offset, byte[] operand, int width) {
        requireRange(target, offset, width);
        byte[] exact = leftPad(operand, width);
        System.arraycopy(exact, 0, target, offset, width);
        return target;
    }

    private static byte[] leftPad(byte[] value, int width) {
        if (value.length > width) {
            throw new IllegalArgumentException("operand width");
        }
        byte[] result = new byte[width];
        System.arraycopy(value, 0, result, width - value.length, value.length);
        return result;
    }

    private static int exactInt(byte[] value) {
        if (value.length != 4) {
            throw new IllegalArgumentException("row offset operand width");
        }
        return ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    private static byte[] swapRows(byte[] value, int first, int second, int width) {
        requireRange(value, first, width);
        requireRange(value, second, width);
        for (int i = 0; i < width; i++) {
            byte x = value[first + i];
            value[first + i] = value[second + i];
            value[second + i] = x;
        }
        return value;
    }

    private static byte[] duplicateRow(byte[] value, int offset, int width) {
        requireRange(value, offset, width);
        byte[] result = new byte[value.length + width];
        System.arraycopy(value, 0, result, 0, offset + width);
        System.arraycopy(value, offset, result, offset + width, width);
        System.arraycopy(value, offset + width, result, offset + 2 * width, value.length - offset - width);
        return result;
    }

    private static byte[] removeRow(byte[] value, int offset, int width) {
        requireRange(value, offset, width);
        byte[] result = new byte[value.length - width];
        System.arraycopy(value, 0, result, 0, offset);
        System.arraycopy(value, offset + width, result, offset, value.length - offset - width);
        return result;
    }

    private static void requireRange(byte[] value, int offset, int width) {
        if (offset < 0 || width < 0 || offset > value.length - width) {
            throw new IllegalArgumentException("mutation range");
        }
    }
}
