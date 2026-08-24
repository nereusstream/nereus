/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32C;

/** Exact test-only inputs and computed component inventory for NWG1_WIRE_GOLDEN_V1. */
final class Nwg1GoldenCorpusV1 {
    static final List<String> VECTOR_IDS = List.of(
            "NWG1_KAFKA_MIN_ZERO_RECORD_NONE_V1",
            "NWG1_KAFKA_MULTI_BINDING_COMMIT_SET_NONE_V1",
            "NWG1_KAFKA_FIXED_ZSTD_V1",
            "NWG1_PULSAR_MIN_ZERO_BYTE_NONE_V1",
            "NWG1_PULSAR_MULTI_BINDING_ADJACENT_NONE_V1",
            "NWG1_PULSAR_FIXED_ZSTD_V1");
    static final List<String> COMPONENT_KINDS = List.of(
            "PROTOCOL_CELL_COMMITMENT",
            "WRAPPED_ENVELOPE_COMMITMENT",
            "OWNER_FENCE_COMMITMENT",
            "HEADER",
            "HKDF_INFO",
            "OBJECT_AEAD_KEY",
            "DIRECTORY_NONCE",
            "DIRECTORY_PLAINTEXT",
            "DIRECTORY_AAD",
            "DIRECTORY_CIPHERTEXT_AND_TAG",
            "FRAME_PRE_AEAD",
            "FRAME_NONCE",
            "FRAME_AAD",
            "FRAME_CIPHERTEXT_AND_TAG",
            "CANONICAL_BODY",
            "LEAF_UTF8");
    static final byte[] WAL_RUN_KEY = Nwg1PositiveManifestV1.testWalRunKey();
    static final byte[] KAFKA_FIXED_ZSTD = Nwg1PositiveManifestV1.zstdFixtureFrame("KAFKA_ZSTD_STANDARD_FRAME_V1");
    static final byte[] PULSAR_FIXED_ZSTD = Nwg1PositiveManifestV1.zstdFixtureFrame("PULSAR_ZSTD_STANDARD_FRAME_V1");
    static final byte[] PULSAR_ZSTD_DECODED = Nwg1PositiveManifestV1.decodedPayload("NWG1_PULSAR_FIXED_ZSTD_V1", 0);

    record Component(String vectorId, String kind, int ordinal, byte[] bytes) {
        Component {
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }

    record Vector(
            String id,
            GroupEncodingPlanV1 plan,
            Nwg1SealedObjectV1 sealed,
            Nwg1VerificationContextV1 verificationContext,
            byte[] walRunKey,
            List<Component> components) {
        Vector {
            walRunKey = walRunKey.clone();
        }

        @Override
        public byte[] walRunKey() {
            return walRunKey.clone();
        }
    }

    private Nwg1GoldenCorpusV1() {}

    static List<Vector> vectors() {
        return Nwg1PositiveManifestV1.loadVectors();
    }

    /** Exact semantic input projection that the tracked JCS manifest must own. */
    static List<Object> positiveInputProjection() {
        List<Object> result = new ArrayList<>();
        for (Vector vector : vectors()) {
            GroupEncodingPlanV1 plan = vector.plan();
            Map<String, Object> input = new LinkedHashMap<>();
            input.put("vectorId", vector.id());
            input.put(
                    "externalFixtureId",
                    vector.id().contains("KAFKA") ? "EXT_KAFKA_WALRUN_AUTHORITY_V1" : "EXT_PULSAR_WALRUN_AUTHORITY_V1");
            input.put("protocolKind", (long) plan.protocolKind());
            input.put("shardId", plan.shardId());
            input.put("shardRunEpoch", plan.shardRunEpoch());
            input.put("laneId", (long) plan.laneId());
            input.put("laneSequence", vector.sealed().header().laneSequence());
            input.put("packingPolicyVersion", (long) plan.packingPolicyVersion());
            input.put("resolvedTargetPayloadBytes", plan.resolvedTargetBytes());
            input.put("resolvedLingerNanos", plan.resolvedLingerNanos());
            input.put("actualCloseLingerNanos", plan.actualCloseLingerNanos());
            input.put("actualCloseReason", (long) plan.closeReason());
            input.put(
                    "syntheticPriorLaneState",
                    Map.of(
                            "nextLaneSequence",
                            vector.sealed().header().laneSequence(),
                            "predecessorObjectCorpusClaim",
                            false));

            List<Object> bindingInputs = new ArrayList<>();
            for (Nwg1DirectoryV1.BindingContext binding : plan.bindings()) {
                Map<String, Object> value = new LinkedHashMap<>();
                value.put("nti1Hex", HexFormat.of().formatHex(binding.nti1Bytes()));
                value.put("ownerFenceKind", (long) binding.ownerFenceKind());
                value.put("ownerFenceVersion", (long) binding.ownerFenceVersion());
                value.put("positionDomainKind", (long) binding.positionDomainKind());
                value.put("positionDomainVersion", (long) binding.positionDomainVersion());
                value.put("framePolicyKind", (long) binding.framePolicyKind());
                value.put("framePolicyVersion", (long) binding.framePolicyVersion());
                value.put(
                        "ownerWitnessHex",
                        HexFormat.of()
                                .formatHex(vector.verificationContext()
                                        .ownerWitnessProvider()
                                        .canonicalWitness(
                                                binding.bindingId(),
                                                binding.ownerFenceKind(),
                                                binding.ownerFenceVersion())));
                bindingInputs.add(value);
            }
            input.put("bindings", bindingInputs);

            List<Object> unitInputs = new ArrayList<>();
            for (Nwg1DirectoryV1.AppendUnit unit : plan.appendUnits()) {
                Map<String, Object> value = new LinkedHashMap<>();
                value.put("contextOrdinal", unit.contextOrdinal());
                value.put("firstFrameOrdinal", unit.firstFrameOrdinal());
                value.put("frameCount", unit.frameCount());
                value.put("appendCommitSetIdHex", HexFormat.of().formatHex(unit.appendCommitSetId()));
                value.put("storageAttemptIdHex", HexFormat.of().formatHex(unit.storageAttemptId()));
                if (unit instanceof Nwg1DirectoryV1.KafkaAppendUnit kafka) {
                    value.put("unitKind", "KAFKA");
                    value.put("partitionId", kafka.partitionId());
                    value.put("kafkaLeaderEpoch", kafka.kafkaLeaderEpoch());
                    value.put("startOffset", kafka.startOffset());
                    value.put("endOffsetExclusive", kafka.endOffsetExclusive());
                } else if (unit instanceof Nwg1DirectoryV1.PulsarAppendUnit pulsar) {
                    value.put("unitKind", "PULSAR");
                    value.put("virtualLedgerId", pulsar.virtualLedgerId());
                    value.put("entryId", pulsar.entryId());
                }
                unitInputs.add(value);
            }
            input.put("appendUnits", unitInputs);

            List<Object> frameInputs = new ArrayList<>();
            for (int ordinal = 0; ordinal < plan.frames().size(); ordinal++) {
                GroupEncodingPlanV1.PlannedFrame frame = plan.frames().get(ordinal);
                Map<String, Object> value = new LinkedHashMap<>();
                value.put("appendUnitOrdinal", frame.appendUnitOrdinal());
                value.put("decodedPayloadHex", HexFormat.of().formatHex(frame.decodedPayload()));
                value.put("coverage0", frame.coverage0());
                value.put("coverage1", frame.coverage1());
                value.put("actualCodecKind", (long) frame.actualCodecKind());
                value.put("actualCodecVersion", (long) frame.actualCodecVersion());
                if (frame.actualCodecKind() == Nwg1ConstantsV1.CODEC_NONE_KIND) {
                    value.put("preAeadRelationship", "EXACT_DECODED_PAYLOAD");
                } else {
                    value.put(
                            "preAeadFixtureId",
                            vector.id().contains("KAFKA")
                                    ? "KAFKA_ZSTD_STANDARD_FRAME_V1"
                                    : "PULSAR_ZSTD_STANDARD_FRAME_V1");
                }
                frameInputs.add(value);
            }
            input.put("frames", frameInputs);
            result.add(input);
        }
        return List.copyOf(result);
    }

    static List<Component> components(
            String id, GroupEncodingPlanV1 plan, Nwg1SealedObjectV1 sealed, byte[] walRunKey) {
        List<Component> result = new ArrayList<>();
        byte[] body = sealed.body();
        byte[] header = Arrays.copyOfRange(body, 0, 256);
        byte[] info = Nwg1CryptoV1.objectKeyInfo(
                plan.shardId(),
                plan.shardRunEpoch(),
                plan.laneId(),
                sealed.header().laneSequence());
        byte[] key = Nwg1CryptoV1.deriveObjectAeadKey(walRunKey, plan.rootSha256(), info);
        byte[] directory = Nwg1DirectoryCodecV1.encode(sealed.directory());
        add(result, id, "PROTOCOL_CELL_COMMITMENT", 0, plan.protocolCellCommitment());
        add(result, id, "WRAPPED_ENVELOPE_COMMITMENT", 0, plan.envelopeCommitment());
        for (int i = 0; i < plan.bindings().size(); i++) {
            add(result, id, "OWNER_FENCE_COMMITMENT", i, plan.bindings().get(i).ownerFenceCommitment());
        }
        add(result, id, "HEADER", 0, header);
        add(result, id, "HKDF_INFO", 0, info);
        add(result, id, "OBJECT_AEAD_KEY", 0, key);
        add(result, id, "DIRECTORY_NONCE", 0, Nwg1CryptoV1.directoryNonce());
        add(result, id, "DIRECTORY_PLAINTEXT", 0, directory);
        add(result, id, "DIRECTORY_AAD", 0, Nwg1CryptoV1.directoryAad(header));
        add(
                result,
                id,
                "DIRECTORY_CIPHERTEXT_AND_TAG",
                0,
                Arrays.copyOfRange(body, 256, Math.toIntExact(sealed.header().directoryPrefixEnd())));
        for (int i = 0; i < plan.frames().size(); i++) {
            Nwg1DirectoryV1.Frame row = sealed.directory().frames().get(i);
            add(result, id, "FRAME_PRE_AEAD", i, plan.frames().get(i).preAeadBytes());
            add(result, id, "FRAME_NONCE", i, Nwg1CryptoV1.frameNonce(i));
            add(
                    result,
                    id,
                    "FRAME_AAD",
                    i,
                    Nwg1CryptoV1.frameAad(header, i, Nwg1DirectoryCodecV1.frameRowBytes(sealed.directory(), i)));
            add(
                    result,
                    id,
                    "FRAME_CIPHERTEXT_AND_TAG",
                    i,
                    Arrays.copyOfRange(
                            body,
                            Math.toIntExact(row.storedBodyOffset()),
                            Math.toIntExact(row.storedBodyOffset() + row.storedBlockBytes())));
        }
        add(result, id, "CANONICAL_BODY", 0, body);
        add(result, id, "LEAF_UTF8", 0, sealed.leafUtf8().getBytes(StandardCharsets.UTF_8));
        return List.copyOf(result);
    }

    private static void add(List<Component> target, String id, String kind, int ordinal, byte[] bytes) {
        target.add(new Component(id, kind, ordinal, bytes));
    }

    static byte[] kafkaBatch(int length, long baseOffset, int seed) {
        if (length < 61) {
            throw new IllegalArgumentException("Kafka batch length");
        }
        byte[] batch = new byte[length];
        ByteBuffer out = ByteBuffer.wrap(batch).order(ByteOrder.BIG_ENDIAN);
        out.putLong(baseOffset)
                .putInt(length - 12)
                .putInt(7)
                .put((byte) 2)
                .putInt(0)
                .putShort((short) 0)
                .putInt(0)
                .putLong(1_700_000_000_000L + seed)
                .putLong(1_700_000_000_000L + seed)
                .putLong(-1)
                .putShort((short) -1)
                .putInt(-1)
                .putInt(0);
        while (out.hasRemaining()) {
            out.put((byte) 'A');
        }
        CRC32C crc = new CRC32C();
        crc.update(batch, 21, batch.length - 21);
        ByteBuffer.wrap(batch).order(ByteOrder.BIG_ENDIAN).putInt(17, (int) crc.getValue());
        return batch;
    }

    static byte[] hex(String value) {
        return HexFormat.of().parseHex(value);
    }
}
