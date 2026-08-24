/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Strict loader that makes the tracked JCS manifest the positive-vector input authority. */
final class Nwg1PositiveManifestV1 {
    private static final Set<String> VECTOR_KEYS = Set.of(
            "actualCloseLingerNanos",
            "actualCloseReason",
            "appendUnits",
            "bindings",
            "externalFixtureId",
            "frames",
            "laneId",
            "laneSequence",
            "packingPolicyVersion",
            "protocolKind",
            "resolvedLingerNanos",
            "resolvedTargetPayloadBytes",
            "shardId",
            "shardRunEpoch",
            "syntheticPriorLaneState",
            "vectorId");
    private static final Set<String> BINDING_KEYS = Set.of(
            "framePolicyKind",
            "framePolicyVersion",
            "nti1Hex",
            "ownerFenceKind",
            "ownerFenceVersion",
            "ownerWitnessHex",
            "positionDomainKind",
            "positionDomainVersion");
    private static final Set<String> KAFKA_UNIT_KEYS = Set.of(
            "appendCommitSetIdHex",
            "contextOrdinal",
            "endOffsetExclusive",
            "firstFrameOrdinal",
            "frameCount",
            "kafkaLeaderEpoch",
            "partitionId",
            "startOffset",
            "storageAttemptIdHex",
            "unitKind");
    private static final Set<String> PULSAR_UNIT_KEYS = Set.of(
            "appendCommitSetIdHex",
            "contextOrdinal",
            "entryId",
            "firstFrameOrdinal",
            "frameCount",
            "storageAttemptIdHex",
            "unitKind",
            "virtualLedgerId");
    private static final Set<String> FRAME_NONE_KEYS = Set.of(
            "actualCodecKind",
            "actualCodecVersion",
            "appendUnitOrdinal",
            "coverage0",
            "coverage1",
            "decodedPayloadHex",
            "preAeadRelationship");
    private static final Set<String> FRAME_ZSTD_KEYS = Set.of(
            "actualCodecKind",
            "actualCodecVersion",
            "appendUnitOrdinal",
            "coverage0",
            "coverage1",
            "decodedPayloadHex",
            "preAeadFixtureId");
    private static final Set<String> EXTERNAL_FIXTURE_KEYS = Set.of(
            "canonicalEnvelopeHex",
            "envelopeKind",
            "envelopeVersion",
            "fixtureId",
            "npc1Hex",
            "provenance",
            "providerScopeIdHex",
            "rootWireFrozen",
            "syntheticRootAuthority",
            "testOnlyPlaintextWalRunKeyHex",
            "walRunRootSha256Hex");
    private static final Set<String> ZSTD_FIXTURE_KEYS =
            Set.of("decodedLength", "decodedSha256", "fixtureId", "frameHex", "provenanceTool");

    private Nwg1PositiveManifestV1() {}

    static List<Nwg1GoldenCorpusV1.Vector> loadVectors() {
        Map<String, Object> manifest = manifest();
        Map<String, ExternalFixture> external = externalFixtures(manifest);
        Map<String, ZstdFixture> zstd = zstdFixtures(manifest);
        List<Nwg1GoldenCorpusV1.Vector> result = new ArrayList<>();
        List<Object> authored = list(manifest.get("vectors"));
        if (authored.size() != Nwg1GoldenCorpusV1.VECTOR_IDS.size()) {
            throw new IllegalArgumentException("positive vector count");
        }
        for (int ordinal = 0; ordinal < authored.size(); ordinal++) {
            Map<String, Object> input = object(authored.get(ordinal));
            exactKeys(input, VECTOR_KEYS, "positive vector");
            String vectorId = string(input.get("vectorId"));
            if (!vectorId.equals(Nwg1GoldenCorpusV1.VECTOR_IDS.get(ordinal))) {
                throw new IllegalArgumentException("positive vector order/id");
            }
            ExternalFixture fixture = required(external, string(input.get("externalFixtureId")), "external fixture");
            int protocolKind = exactInt(input.get("protocolKind"));
            if (protocolKind != fixture.protocolCell().protocolKind().code()) {
                throw new IllegalArgumentException("vector/fixture protocol mismatch");
            }
            List<GroupEncodingPlanV1.PlannedFrame> frames = frames(input, zstd);
            Map<String, byte[]> ownerWitnesses = new LinkedHashMap<>();
            List<Nwg1DirectoryV1.BindingContext> bindings = bindings(input, fixture, ownerWitnesses);
            List<Nwg1DirectoryV1.AppendUnit> units = appendUnits(input, protocolKind, frames);
            Map<String, Object> prior = object(input.get("syntheticPriorLaneState"));
            exactKeys(prior, Set.of("nextLaneSequence", "predecessorObjectCorpusClaim"), "prior lane state");
            long laneSequence = number(input.get("laneSequence"));
            if (number(prior.get("nextLaneSequence")) != laneSequence
                    || bool(prior.get("predecessorObjectCorpusClaim"))) {
                throw new IllegalArgumentException("synthetic prior-lane claim");
            }
            GroupEncodingPlanV1 plan = new GroupEncodingPlanV1(
                    protocolKind,
                    number(input.get("shardId")),
                    number(input.get("shardRunEpoch")),
                    exactInt(input.get("laneId")),
                    exactInt(input.get("packingPolicyVersion")),
                    number(input.get("resolvedTargetPayloadBytes")),
                    number(input.get("resolvedLingerNanos")),
                    number(input.get("actualCloseLingerNanos")),
                    exactInt(input.get("actualCloseReason")),
                    Nwg1CommitmentsV1.protocolCell(fixture.npc1()),
                    fixture.providerScopeId(),
                    fixture.walRunRootSha256(),
                    Nwg1CommitmentsV1.wrappedEnvelope(fixture.envelope()),
                    bindings,
                    units,
                    frames);
            Nwg1VerificationContextV1 context = new Nwg1VerificationContextV1(
                    fixture.protocolCell(),
                    fixture.providerScopeId(),
                    fixture.walRunRootSha256(),
                    fixture.envelope(),
                    (bindingId, kind, version) -> {
                        byte[] witness = ownerWitnesses.get(HexFormat.of().formatHex(bindingId));
                        if (witness == null) {
                            throw new IllegalArgumentException("unknown Binding witness");
                        }
                        return witness.clone();
                    },
                    new Nwg1StrictNativePayloadVerifierV1(),
                    0,
                    protocolKind == 2 ? 1L << 40 : 0);
            Nwg1SealedObjectV1 sealed = Nwg1ObjectWriterV1.seal(plan, laneSequence, fixture.walRunKey(), context);
            result.add(new Nwg1GoldenCorpusV1.Vector(
                    vectorId,
                    plan,
                    sealed,
                    context,
                    fixture.walRunKey(),
                    Nwg1GoldenCorpusV1.components(vectorId, plan, sealed, fixture.walRunKey())));
        }
        return List.copyOf(result);
    }

    static Map<String, Object> manifest() {
        try {
            byte[] bytes = Files.readAllBytes(manifestPath());
            if (bytes.length == 0 || bytes.length > 1_048_576) {
                throw new IllegalArgumentException("NWG1 manifest exceeds dedicated 1 MiB cap");
            }
            return object(StrictJcsV1.parseCanonical(bytes));
        } catch (IOException e) {
            throw new IllegalStateException("cannot read NWG1 manifest", e);
        }
    }

    static byte[] testWalRunKey() {
        Map<String, ExternalFixture> fixtures = externalFixtures(manifest());
        byte[] kafka = required(fixtures, "EXT_KAFKA_WALRUN_AUTHORITY_V1", "external fixture")
                .walRunKey();
        byte[] pulsar = required(fixtures, "EXT_PULSAR_WALRUN_AUTHORITY_V1", "external fixture")
                .walRunKey();
        if (!Arrays.equals(kafka, pulsar)) {
            throw new IllegalArgumentException("positive fixtures use different test WalRun keys");
        }
        return kafka;
    }

    static byte[] zstdFixtureFrame(String fixtureId) {
        return required(zstdFixtures(manifest()), fixtureId, "ZSTD fixture").frame();
    }

    static byte[] decodedPayload(String vectorId, int frameOrdinal) {
        for (Object value : list(manifest().get("vectors"))) {
            Map<String, Object> vector = object(value);
            if (vectorId.equals(vector.get("vectorId"))) {
                List<Object> frames = list(vector.get("frames"));
                if (frameOrdinal < 0 || frameOrdinal >= frames.size()) {
                    throw new IllegalArgumentException("Frame ordinal");
                }
                return hex(object(frames.get(frameOrdinal)).get("decodedPayloadHex"));
            }
        }
        throw new IllegalArgumentException("unknown positive vector");
    }

    private static Map<String, ExternalFixture> externalFixtures(Map<String, Object> manifest) {
        Map<String, ExternalFixture> result = new LinkedHashMap<>();
        for (Object value : list(manifest.get("externalFixtures"))) {
            Map<String, Object> input = object(value);
            exactKeys(input, EXTERNAL_FIXTURE_KEYS, "external fixture");
            String id = string(input.get("fixtureId"));
            if (!string(input.get("provenance")).equals("TEST_ONLY_NON_SECRET")
                    || !bool(input.get("syntheticRootAuthority"))
                    || bool(input.get("rootWireFrozen"))) {
                throw new IllegalArgumentException("external fixture provenance flags");
            }
            int kind = exactInt(input.get("envelopeKind"));
            int version = exactInt(input.get("envelopeVersion"));
            if (kind != Nwg1EnvelopeV1.KIND || version != Nwg1EnvelopeV1.VERSION) {
                throw new IllegalArgumentException("external envelope kind/version");
            }
            byte[] canonicalEnvelope = hex(input.get("canonicalEnvelopeHex"));
            byte[] framedEnvelope = ByteBuffer.allocate(8 + canonicalEnvelope.length)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putShort((short) kind)
                    .putShort((short) version)
                    .putInt(canonicalEnvelope.length)
                    .put(canonicalEnvelope)
                    .array();
            ExternalFixture fixture = new ExternalFixture(
                    hex(input.get("npc1Hex")),
                    hexExact(input.get("providerScopeIdHex"), 32),
                    hexExact(input.get("walRunRootSha256Hex"), 32),
                    Nwg1EnvelopeV1.decode(framedEnvelope),
                    hexExact(input.get("testOnlyPlaintextWalRunKeyHex"), 32));
            if (Arrays.equals(fixture.walRunRootSha256(), new byte[32])) {
                throw new IllegalArgumentException("zero synthetic Root SHA");
            }
            if (result.put(id, fixture) != null) {
                throw new IllegalArgumentException("duplicate external fixture");
            }
        }
        if (!result.keySet().equals(Set.of("EXT_KAFKA_WALRUN_AUTHORITY_V1", "EXT_PULSAR_WALRUN_AUTHORITY_V1"))) {
            throw new IllegalArgumentException("external fixture inventory");
        }
        return result;
    }

    private static Map<String, ZstdFixture> zstdFixtures(Map<String, Object> manifest) {
        Map<String, ZstdFixture> result = new LinkedHashMap<>();
        for (Object value : list(manifest.get("zstdFixtures"))) {
            Map<String, Object> input = object(value);
            exactKeys(input, ZSTD_FIXTURE_KEYS, "ZSTD fixture");
            String provenance = string(input.get("provenanceTool"));
            if (!provenance.equals("zstd-cli-1.5.7 --level=1 --content-size")) {
                throw new IllegalArgumentException("ZSTD provenance");
            }
            byte[] frame = hex(input.get("frameHex"));
            String id = string(input.get("fixtureId"));
            ZstdFixture fixture = new ZstdFixture(
                    frame,
                    Math.toIntExact(number(input.get("decodedLength"))),
                    hexExact(input.get("decodedSha256"), 32));
            if (fixture.decodedLength() <= 0 || result.put(id, fixture) != null) {
                throw new IllegalArgumentException("duplicate ZSTD fixture");
            }
        }
        if (!result.keySet().equals(Set.of("KAFKA_ZSTD_STANDARD_FRAME_V1", "PULSAR_ZSTD_STANDARD_FRAME_V1"))) {
            throw new IllegalArgumentException("ZSTD fixture inventory");
        }
        return result;
    }

    private static List<Nwg1DirectoryV1.BindingContext> bindings(
            Map<String, Object> vector, ExternalFixture fixture, Map<String, byte[]> ownerWitnesses) {
        List<Nwg1DirectoryV1.BindingContext> result = new ArrayList<>();
        for (Object value : list(vector.get("bindings"))) {
            Map<String, Object> input = object(value);
            exactKeys(input, BINDING_KEYS, "Binding input");
            byte[] nti1 = hex(input.get("nti1Hex"));
            TopicIncarnationIdentity incarnation = TopicIncarnationIdentityCodecV1.decode(nti1);
            byte[] bindingId = DeterministicTopicIdsV1.deriveBindingId(fixture.protocolCell(), incarnation)
                    .digest()
                    .bytes()
                    .toByteArray();
            byte[] storageEpochId = DeterministicTopicIdsV1.deriveStorageEpochId(
                            DeterministicTopicIdsV1.deriveBindingId(fixture.protocolCell(), incarnation), 0)
                    .digest()
                    .bytes()
                    .toByteArray();
            int ownerKind = exactInt(input.get("ownerFenceKind"));
            int ownerVersion = exactInt(input.get("ownerFenceVersion"));
            byte[] witness = hex(input.get("ownerWitnessHex"));
            String bindingHex = HexFormat.of().formatHex(bindingId);
            if (ownerWitnesses.put(bindingHex, witness) != null) {
                throw new IllegalArgumentException("duplicate Binding identity");
            }
            result.add(new Nwg1DirectoryV1.BindingContext(
                    bindingId,
                    storageEpochId,
                    Nwg1CommitmentsV1.ownerFence(ownerKind, ownerVersion, witness),
                    nti1,
                    ownerKind,
                    ownerVersion,
                    exactInt(input.get("positionDomainKind")),
                    exactInt(input.get("positionDomainVersion")),
                    exactInt(input.get("framePolicyKind")),
                    exactInt(input.get("framePolicyVersion"))));
        }
        return List.copyOf(result);
    }

    private static List<GroupEncodingPlanV1.PlannedFrame> frames(
            Map<String, Object> vector, Map<String, ZstdFixture> zstdFixtures) {
        List<GroupEncodingPlanV1.PlannedFrame> result = new ArrayList<>();
        for (Object value : list(vector.get("frames"))) {
            Map<String, Object> input = object(value);
            int codecKind = exactInt(input.get("actualCodecKind"));
            int codecVersion = exactInt(input.get("actualCodecVersion"));
            byte[] decoded = hex(input.get("decodedPayloadHex"));
            byte[] preAead;
            if (codecKind == 0 && codecVersion == 0) {
                exactKeys(input, FRAME_NONE_KEYS, "NONE Frame input");
                if (!string(input.get("preAeadRelationship")).equals("EXACT_DECODED_PAYLOAD")) {
                    throw new IllegalArgumentException("NONE pre-AEAD relationship");
                }
                preAead = decoded;
            } else if (codecKind == 1 && codecVersion == 1) {
                exactKeys(input, FRAME_ZSTD_KEYS, "ZSTD Frame input");
                ZstdFixture fixture = required(zstdFixtures, string(input.get("preAeadFixtureId")), "ZSTD fixture");
                if (decoded.length != fixture.decodedLength()
                        || !Arrays.equals(Nwg1CommitmentsV1.sha256(decoded), fixture.decodedSha256())) {
                    throw new IllegalArgumentException("ZSTD decoded facts mismatch");
                }
                preAead = fixture.frame();
            } else {
                throw new IllegalArgumentException("positive vector codec");
            }
            result.add(new GroupEncodingPlanV1.PlannedFrame(
                    number(input.get("appendUnitOrdinal")),
                    decoded,
                    preAead,
                    number(input.get("coverage0")),
                    number(input.get("coverage1")),
                    codecKind,
                    codecVersion));
        }
        return List.copyOf(result);
    }

    private static List<Nwg1DirectoryV1.AppendUnit> appendUnits(
            Map<String, Object> vector, int protocolKind, List<GroupEncodingPlanV1.PlannedFrame> frames) {
        List<Nwg1DirectoryV1.AppendUnit> result = new ArrayList<>();
        for (Object value : list(vector.get("appendUnits"))) {
            Map<String, Object> input = object(value);
            long unitOrdinal = result.size();
            byte[] assigned = assignedDigest(frames, unitOrdinal);
            String kind = string(input.get("unitKind"));
            if (kind.equals("KAFKA") && protocolKind == 1) {
                exactKeys(input, KAFKA_UNIT_KEYS, "Kafka AppendUnit input");
                result.add(new Nwg1DirectoryV1.KafkaAppendUnit(
                        number(input.get("contextOrdinal")),
                        number(input.get("firstFrameOrdinal")),
                        number(input.get("frameCount")),
                        number(input.get("partitionId")),
                        number(input.get("kafkaLeaderEpoch")),
                        number(input.get("startOffset")),
                        number(input.get("endOffsetExclusive")),
                        hexExact(input.get("appendCommitSetIdHex"), 16),
                        hexExact(input.get("storageAttemptIdHex"), 16),
                        assigned));
            } else if (kind.equals("PULSAR") && protocolKind == 2) {
                exactKeys(input, PULSAR_UNIT_KEYS, "Pulsar AppendUnit input");
                result.add(new Nwg1DirectoryV1.PulsarAppendUnit(
                        number(input.get("contextOrdinal")),
                        number(input.get("firstFrameOrdinal")),
                        number(input.get("frameCount")),
                        number(input.get("virtualLedgerId")),
                        number(input.get("entryId")),
                        hexExact(input.get("appendCommitSetIdHex"), 16),
                        hexExact(input.get("storageAttemptIdHex"), 16),
                        assigned));
            } else {
                throw new IllegalArgumentException("AppendUnit protocol/kind mismatch");
            }
        }
        return List.copyOf(result);
    }

    private static byte[] assignedDigest(List<GroupEncodingPlanV1.PlannedFrame> frames, long unitOrdinal) {
        ByteArrayOutputStream joined = new ByteArrayOutputStream();
        for (GroupEncodingPlanV1.PlannedFrame frame : frames) {
            if (frame.appendUnitOrdinal() == unitOrdinal) {
                joined.writeBytes(frame.decodedPayload());
            }
        }
        return Nwg1CommitmentsV1.sha256(joined.toByteArray());
    }

    private static Path manifestPath() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve("docs/v2/wire/nwg1-v1-golden-manifest.json");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("cannot locate NWG1 manifest");
    }

    private static byte[] hex(Object value) {
        String text = string(value);
        if ((text.length() & 1) != 0 || !text.matches("[0-9a-f]*")) {
            throw new IllegalArgumentException("non-canonical lowercase hex");
        }
        return HexFormat.of().parseHex(text);
    }

    private static byte[] hexExact(Object value, int length) {
        byte[] decoded = hex(value);
        if (decoded.length != length) {
            throw new IllegalArgumentException("hex width");
        }
        return decoded;
    }

    private static long number(Object value) {
        return (Long) value;
    }

    private static int exactInt(Object value) {
        return Math.toIntExact(number(value));
    }

    private static boolean bool(Object value) {
        return (Boolean) value;
    }

    private static String string(Object value) {
        return (String) value;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object value) {
        return (List<Object>) value;
    }

    private static void exactKeys(Map<String, Object> value, Set<String> expected, String name) {
        if (!value.keySet().equals(expected)) {
            throw new IllegalArgumentException(name + " fields");
        }
    }

    private static <T> T required(Map<String, T> values, String key, String name) {
        T value = values.get(key);
        if (value == null) {
            throw new IllegalArgumentException("unknown " + name);
        }
        return value;
    }

    private record ExternalFixture(
            byte[] npc1,
            byte[] providerScopeId,
            byte[] walRunRootSha256,
            Nwg1EnvelopeV1 envelope,
            byte[] walRunKey,
            ProtocolCellIdentity protocolCell) {
        private ExternalFixture(
                byte[] npc1,
                byte[] providerScopeId,
                byte[] walRunRootSha256,
                Nwg1EnvelopeV1 envelope,
                byte[] walRunKey) {
            this(
                    npc1.clone(),
                    providerScopeId.clone(),
                    walRunRootSha256.clone(),
                    envelope,
                    walRunKey.clone(),
                    ProtocolCellIdentityCodecV1.decode(npc1));
        }

        @Override
        public byte[] npc1() {
            return npc1.clone();
        }

        @Override
        public byte[] providerScopeId() {
            return providerScopeId.clone();
        }

        @Override
        public byte[] walRunRootSha256() {
            return walRunRootSha256.clone();
        }

        @Override
        public byte[] walRunKey() {
            return walRunKey.clone();
        }
    }

    private record ZstdFixture(byte[] frame, int decodedLength, byte[] decodedSha256) {
        private ZstdFixture {
            frame = frame.clone();
            decodedSha256 = decodedSha256.clone();
        }

        @Override
        public byte[] frame() {
            return frame.clone();
        }

        @Override
        public byte[] decodedSha256() {
            return decodedSha256.clone();
        }
    }
}
