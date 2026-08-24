/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.github.luben.zstd.ZstdInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class Nwg1WireGoldenV1Test {
    @Test
    void exactCorpusHasClosedCountsAndRoundTrips() {
        var vectors = Nwg1GoldenCorpusV1.vectors();
        assertThat(vectors).hasSize(6);
        assertThat(vectors.stream().flatMap(vector -> vector.components().stream()))
                .hasSize(114);
        assertThat(vectors.stream()
                        .flatMap(vector -> vector.components().stream())
                        .map(Nwg1GoldenCorpusV1.Component::kind)
                        .collect(java.util.stream.Collectors.toSet()))
                .containsExactlyInAnyOrderElementsOf(Nwg1GoldenCorpusV1.COMPONENT_KINDS);
        assertThat(vectors.stream()
                        .mapToInt(vector -> vector.plan().bindings().size())
                        .sum())
                .isEqualTo(8);
        assertThat(vectors.stream()
                        .mapToInt(vector -> vector.plan().frames().size())
                        .sum())
                .isEqualTo(10);
        for (var vector : vectors) {
            var decoded = Nwg1ObjectReaderV1.read(
                    vector.sealed().body(),
                    vector.sealed().bodySha256(),
                    vector.verificationContext(),
                    Nwg1GoldenCorpusV1.WAL_RUN_KEY);
            assertThat(decoded.decodedFrames()).hasSameSizeAs(vector.plan().frames());
            for (int i = 0; i < decoded.decodedFrames().size(); i++) {
                assertThat(decoded.decodedFrames().get(i))
                        .isEqualTo(vector.plan().frames().get(i).decodedPayload());
            }
        }
    }

    @Test
    void authenticatedPrefixAndSelectedAppendUnitRangesMatchEveryGolden() throws IOException {
        for (var vector : Nwg1GoldenCorpusV1.vectors()) {
            byte[] body = vector.sealed().body();
            int prefixEnd = Math.toIntExact(vector.sealed().header().directoryPrefixEnd());
            Nwg1ObjectReaderV1.AuthenticatedPrefix prefix = Nwg1ObjectReaderV1.readAuthenticatedPrefix(
                    Arrays.copyOf(body, prefixEnd), body.length, vector.verificationContext(), vector.walRunKey());
            for (Nwg1DirectoryV1.AppendUnit unit : prefix.directory().appendUnits()) {
                long selectedFrame = unit.firstFrameOrdinal();
                List<Nwg1ObjectReaderV1.ExactFrameRange> ranges =
                        Nwg1ObjectReaderV1.selectedAppendUnitRanges(prefix, selectedFrame);
                AtomicInteger sourceIndex = new AtomicInteger();
                java.util.ArrayList<byte[]> decoded = new java.util.ArrayList<>(ranges.size());
                Nwg1ObjectReaderV1.VerifiedAppendUnit verified = Nwg1ObjectReaderV1.readSelectedAppendUnitStreaming(
                        prefix,
                        (range, absoluteFrameOrdinal) -> {
                            assertThat(range).isEqualTo(ranges.get(sourceIndex.getAndIncrement()));
                            return Arrays.copyOfRange(
                                    body,
                                    Math.toIntExact(range.inclusiveStart()),
                                    Math.toIntExact(range.exclusiveEnd()));
                        },
                        selectedFrame,
                        vector.verificationContext(),
                        vector.walRunKey(),
                        (frame, payload) -> {
                            byte[] copy = new byte[payload.remaining()];
                            payload.get(copy);
                            decoded.add(copy);
                        });
                assertThat(verified.frameCount()).isEqualTo(unit.frameCount());
                assertThat(verified.decodedPayloadBytes())
                        .isEqualTo(decoded.stream()
                                .mapToLong(value -> value.length)
                                .sum());
                assertThat(verified.assignedPayloadSha256()).isEqualTo(unit.assignedPayloadSha256());
                assertThat(sourceIndex).hasValue(ranges.size());
                assertThat(decoded).hasSize(Math.toIntExact(unit.frameCount()));
                for (int index = 0; index < decoded.size(); index++) {
                    assertThat(decoded.get(index))
                            .isEqualTo(vector.plan()
                                    .frames()
                                    .get(Math.addExact(Math.toIntExact(unit.firstFrameOrdinal()), index))
                                    .decodedPayload());
                }
            }
        }
    }

    @Test
    void selectedBindingFailureDoesNotConsultSiblingWitness() throws IOException {
        var vector = Nwg1GoldenCorpusV1.vectors().stream()
                .filter(candidate -> candidate.plan().protocolKind() == Nwg1ConstantsV1.PROTOCOL_KAFKA)
                .filter(candidate -> candidate.sealed().directory().bindings().size() > 1)
                .findFirst()
                .orElseThrow();
        Nwg1DirectoryV1.AppendUnit selectedUnit =
                vector.sealed().directory().appendUnits().getFirst();
        byte[] selectedBinding = vector.sealed()
                .directory()
                .bindings()
                .get(Math.toIntExact(selectedUnit.contextOrdinal()))
                .bindingId();
        Nwg1VerificationContextV1 isolated = new Nwg1VerificationContextV1(
                vector.verificationContext().protocolCell(),
                vector.verificationContext().cellProviderScopeId(),
                vector.verificationContext().walRunRootSha256(),
                vector.verificationContext().envelope(),
                (bindingId, kind, version) -> {
                    if (!Arrays.equals(bindingId, selectedBinding)) {
                        throw new IllegalStateException("sibling witness deliberately unavailable");
                    }
                    return vector.verificationContext()
                            .ownerWitnessProvider()
                            .canonicalWitness(bindingId, kind, version);
                },
                vector.verificationContext().nativePayloadVerifier(),
                0,
                0);
        byte[] body = vector.sealed().body();
        int prefixEnd = Math.toIntExact(vector.sealed().header().directoryPrefixEnd());
        Nwg1ObjectReaderV1.AuthenticatedPrefix prefix = Nwg1ObjectReaderV1.readAuthenticatedPrefix(
                Arrays.copyOf(body, prefixEnd), body.length, isolated, vector.walRunKey());
        List<Nwg1ObjectReaderV1.ExactFrameRange> ranges =
                Nwg1ObjectReaderV1.selectedAppendUnitRanges(prefix, selectedUnit.firstFrameOrdinal());
        AtomicInteger consumed = new AtomicInteger();
        Nwg1ObjectReaderV1.VerifiedAppendUnit verified = Nwg1ObjectReaderV1.readSelectedAppendUnitStreaming(
                prefix,
                (range, frameOrdinal) -> Arrays.copyOfRange(
                        body, Math.toIntExact(range.inclusiveStart()), Math.toIntExact(range.exclusiveEnd())),
                selectedUnit.firstFrameOrdinal(),
                isolated,
                vector.walRunKey(),
                (frame, payload) -> consumed.incrementAndGet());

        assertThat(verified.frameCount()).isEqualTo(selectedUnit.frameCount());
        assertThat(consumed).hasValue(Math.toIntExact(selectedUnit.frameCount()));
    }

    @Test
    void streamingFoldConsumesHighCompressionFramesOneAtATimeAndReturnsCompactDigestCoverage() throws Exception {
        StreamingFixture fixture = highCompressionStreamingFixture();
        byte[] body = fixture.sealed().body();
        Nwg1ObjectReaderV1.AuthenticatedPrefix prefix = authenticatedPrefix(fixture, body);
        java.util.ArrayList<byte[]> transferredCiphertexts = new java.util.ArrayList<>();
        java.util.ArrayList<ByteBuffer> borrowedPayloads = new java.util.ArrayList<>();
        MessageDigest consumedDigest = MessageDigest.getInstance("SHA-256");
        AtomicInteger sourceCalls = new AtomicInteger();
        AtomicInteger consumedFrames = new AtomicInteger();
        AtomicInteger nativeValidations = new AtomicInteger();
        long[] consumedBytes = new long[1];
        Nwg1VerificationContextV1 baseContext = fixture.authority().verificationContext();
        Nwg1VerificationContextV1 streamingContext = new Nwg1VerificationContextV1(
                baseContext.protocolCell(),
                baseContext.cellProviderScopeId(),
                baseContext.walRunRootSha256(),
                baseContext.envelope(),
                baseContext.ownerWitnessProvider(),
                (payload, partitionId, leaderEpoch, coverageStart, coverageEnd) -> {
                    nativeValidations.incrementAndGet();
                    return baseContext
                            .nativePayloadVerifier()
                            .validateKafka(payload, partitionId, leaderEpoch, coverageStart, coverageEnd);
                },
                0,
                0);

        Nwg1ObjectReaderV1.VerifiedAppendUnit verified = Nwg1ObjectReaderV1.readSelectedAppendUnitStreaming(
                prefix,
                (range, absoluteFrameOrdinal) -> {
                    if (!transferredCiphertexts.isEmpty()) {
                        assertErased(transferredCiphertexts.getLast());
                        assertErased(borrowedPayloads.getLast());
                    }
                    assertThat(absoluteFrameOrdinal).isEqualTo(sourceCalls.getAndIncrement());
                    byte[] exactCiphertext = Arrays.copyOfRange(
                            body, Math.toIntExact(range.inclusiveStart()), Math.toIntExact(range.exclusiveEnd()));
                    transferredCiphertexts.add(exactCiphertext);
                    return exactCiphertext;
                },
                0,
                streamingContext,
                fixture.authority().walRunKey(),
                (frame, payload) -> {
                    assertThat(frame.appendUnitFrameOrdinal()).isEqualTo(consumedFrames.getAndIncrement());
                    borrowedPayloads.add(payload.duplicate());
                    consumedBytes[0] = Math.addExact(consumedBytes[0], payload.remaining());
                    consumedDigest.update(payload);
                });

        Nwg1DirectoryV1.KafkaAppendUnit unit = (Nwg1DirectoryV1.KafkaAppendUnit)
                fixture.sealed().directory().appendUnits().getFirst();
        assertThat(sourceCalls).hasValue(2);
        assertThat(consumedFrames).hasValue(2);
        assertThat(nativeValidations).hasValue(2);
        assertThat(verified.frameCount()).isEqualTo(2);
        assertThat(verified.decodedPayloadBytes()).isEqualTo(consumedBytes[0]);
        assertThat(verified.coverage0()).isEqualTo(900);
        assertThat(verified.coverage1()).isEqualTo(902);
        assertThat(verified.appendCommitSetId()).isEqualTo(unit.appendCommitSetId());
        assertThat(verified.storageAttemptId()).isEqualTo(unit.storageAttemptId());
        assertThat(verified.assignedPayloadSha256())
                .isEqualTo(unit.assignedPayloadSha256())
                .isEqualTo(consumedDigest.digest());
        transferredCiphertexts.forEach(Nwg1WireGoldenV1Test::assertErased);
        borrowedPayloads.forEach(Nwg1WireGoldenV1Test::assertErased);
    }

    @Test
    void streamingFoldErasesCiphertextAndDecodedPayloadWhenConsumerFails() throws Exception {
        StreamingFixture fixture = highCompressionStreamingFixture();
        byte[] body = fixture.sealed().body();
        Nwg1ObjectReaderV1.AuthenticatedPrefix prefix = authenticatedPrefix(fixture, body);
        AtomicReference<byte[]> transferredCiphertext = new AtomicReference<>();
        AtomicReference<ByteBuffer> borrowedPayload = new AtomicReference<>();
        AtomicInteger sourceCalls = new AtomicInteger();

        assertThatThrownBy(() -> Nwg1ObjectReaderV1.readSelectedAppendUnitStreaming(
                        prefix,
                        (range, absoluteFrameOrdinal) -> {
                            sourceCalls.incrementAndGet();
                            byte[] exactCiphertext = Arrays.copyOfRange(
                                    body,
                                    Math.toIntExact(range.inclusiveStart()),
                                    Math.toIntExact(range.exclusiveEnd()));
                            transferredCiphertext.set(exactCiphertext);
                            return exactCiphertext;
                        },
                        0,
                        fixture.authority().verificationContext(),
                        fixture.authority().walRunKey(),
                        (frame, payload) -> {
                            borrowedPayload.set(payload.duplicate());
                            throw new IOException("injected synchronous consumer failure");
                        }))
                .isInstanceOf(IOException.class)
                .hasMessage("injected synchronous consumer failure");

        assertThat(sourceCalls).hasValue(1);
        assertErased(transferredCiphertext.get());
        assertErased(borrowedPayload.get());
    }

    @Test
    void streamingFoldRejectsAssignedDigestAndCoverageUnionAfterConsumingExactFrames() throws Exception {
        StreamingFixture fixture = highCompressionStreamingFixture();
        Nwg1DirectoryV1 directory = fixture.sealed().directory();
        Nwg1DirectoryV1.KafkaAppendUnit unit =
                (Nwg1DirectoryV1.KafkaAppendUnit) directory.appendUnits().getFirst();
        byte[] wrongDigest = unit.assignedPayloadSha256();
        wrongDigest[0] ^= 1;
        Nwg1DirectoryV1.KafkaAppendUnit digestMismatch = kafkaUnitWithDigest(unit, wrongDigest);
        Nwg1DirectoryV1.KafkaAppendUnit coverageMismatch = new Nwg1DirectoryV1.KafkaAppendUnit(
                unit.contextOrdinal(),
                unit.firstFrameOrdinal(),
                unit.frameCount(),
                unit.partitionId(),
                unit.kafkaLeaderEpoch(),
                unit.startOffset(),
                unit.endOffsetExclusive() + 1,
                unit.appendCommitSetId(),
                unit.storageAttemptId(),
                unit.assignedPayloadSha256());

        assertStreamingRejected(
                fixture,
                resignDirectory(
                        fixture,
                        new Nwg1DirectoryV1(
                                directory.protocolKind(),
                                directory.bindings(),
                                List.of(digestMismatch),
                                directory.frames())),
                Nwg1RejectionV1.DIGEST_MISMATCH);
        assertStreamingRejected(
                fixture,
                resignDirectory(
                        fixture,
                        new Nwg1DirectoryV1(
                                directory.protocolKind(),
                                directory.bindings(),
                                List.of(coverageMismatch),
                                directory.frames())),
                Nwg1RejectionV1.COVERAGE_MISMATCH);
    }

    @Test
    void fixedExternalZstdFramesAreConsumedButNeverGenerated() throws IOException {
        byte[] kafka = Nwg1GoldenCorpusV1.kafkaBatch(152, 300, 0);
        assertThat(Nwg1ZstdV1.decompress(Nwg1GoldenCorpusV1.KAFKA_FIXED_ZSTD, kafka.length))
                .isEqualTo(kafka);
        assertThat(Nwg1ZstdV1.decompress(
                        Nwg1GoldenCorpusV1.PULSAR_FIXED_ZSTD, Nwg1GoldenCorpusV1.PULSAR_ZSTD_DECODED.length))
                .isEqualTo(Nwg1GoldenCorpusV1.PULSAR_ZSTD_DECODED);
        assertThat(independentStreamingDecode(Nwg1GoldenCorpusV1.KAFKA_FIXED_ZSTD))
                .isEqualTo(kafka);
        assertThat(independentStreamingDecode(Nwg1GoldenCorpusV1.PULSAR_FIXED_ZSTD))
                .isEqualTo(Nwg1GoldenCorpusV1.PULSAR_ZSTD_DECODED);
        var productionCandidate = Nwg1ZstdV1.encodeIfSmaller(kafka);
        assertThat(productionCandidate.codecKind()).isEqualTo(Nwg1ConstantsV1.CODEC_ZSTD_KIND);
        assertThat(productionCandidate.codecVersion()).isEqualTo(Nwg1ConstantsV1.CODEC_ZSTD_VERSION);
        assertThat(Nwg1ZstdV1.decompress(productionCandidate.preAeadBytes(), kafka.length))
                .isEqualTo(kafka);
    }

    @Test
    void zstdRejectsTrailingConcatenatedSkippableDictionaryAndMissingContentSize() {
        byte[] frame = Nwg1GoldenCorpusV1.KAFKA_FIXED_ZSTD;
        assertCodecRejected(Arrays.copyOf(frame, frame.length + 1), 152);
        byte[] concatenated = new byte[frame.length * 2];
        System.arraycopy(frame, 0, concatenated, 0, frame.length);
        System.arraycopy(frame, 0, concatenated, frame.length, frame.length);
        assertCodecRejected(concatenated, 152);
        byte[] skippable = frame.clone();
        System.arraycopy(new byte[] {0x50, 0x2a, 0x4d, 0x18}, 0, skippable, 0, 4);
        assertCodecRejected(skippable, 152);
        byte[] dictionary = frame.clone();
        dictionary[4] |= 1;
        assertCodecRejected(dictionary, 152);
        byte[] missingContentSize = frame.clone();
        missingContentSize[4] &= ~0x20;
        assertCodecRejected(missingContentSize, 152);
    }

    @Test
    void headerCrcAndRequiredZeroAreStrict() {
        byte[] header = Nwg1GoldenCorpusV1.vectors().getFirst().components().stream()
                .filter(component -> component.kind().equals("HEADER"))
                .findFirst()
                .orElseThrow()
                .bytes();
        assertThat(Nwg1HeaderCodecV1.encode(Nwg1HeaderCodecV1.decode(header))).isEqualTo(header);
        byte[] corrupted = header.clone();
        corrupted[14] = 1;
        ByteBuffer.wrap(corrupted).order(ByteOrder.BIG_ENDIAN).putInt(252, Nwg1HeaderCodecV1.crc32c(corrupted));
        assertThatThrownBy(() -> Nwg1HeaderCodecV1.decode(corrupted))
                .isInstanceOfSatisfying(Nwg1ValidationException.class, failure -> assertThat(failure.rejection())
                        .isEqualTo(Nwg1RejectionV1.REQUIRED_ZERO_NONZERO));
    }

    @Test
    void hkdfMatchesIndependentRfc5869Computation() {
        byte[] ikm = new byte[32];
        for (int i = 0; i < ikm.length; i++) {
            ikm[i] = (byte) i;
        }
        byte[] salt = new byte[32];
        Arrays.fill(salt, (byte) 0x11);
        byte[] info = Nwg1CryptoV1.objectKeyInfo(7, 1000, 1, 10);
        assertThat(Nwg1CryptoV1.deriveObjectAeadKey(ikm, salt, info))
                .isEqualTo(Nwg1GoldenCorpusV1.hex("ed36370935dda34db96316b353026a0ed28f9400422bb71fb3196f5219728e29"));
    }

    @Test
    void typedInventoriesRemainClosed() {
        assertThat(Nwg1RejectionV1.values()).hasSize(25);
        assertThat(Nwg1ValidationStageV1.values()).hasSize(16);
        assertThat(Nwg1IsolationScopeV1.values()).hasSize(4);
        assertThat(Nwg1CloseReasonV1.values()).hasSize(12);
        for (int code = 1; code <= 12; code++) {
            assertThat(Nwg1CloseReasonV1.fromCode(code).code()).isEqualTo(code);
        }
        assertThat(new HashSet<>(Nwg1GoldenCorpusV1.VECTOR_IDS)).hasSize(6);
    }

    @Test
    void readerRejectsOwnerWitnessBindingDerivationAssignedDigestNativeCrcAndPulsarSlice() {
        var kafka = Nwg1GoldenCorpusV1.vectors().getFirst();
        var wrongOwnerContext = new Nwg1VerificationContextV1(
                kafka.verificationContext().protocolCell(),
                kafka.verificationContext().cellProviderScopeId(),
                kafka.verificationContext().walRunRootSha256(),
                kafka.verificationContext().envelope(),
                (bindingId, kind, version) -> new byte[] {1},
                new Nwg1StrictNativePayloadVerifierV1(),
                0,
                0);
        assertReadRejected(
                kafka.sealed().body(),
                kafka.sealed().bodySha256(),
                wrongOwnerContext,
                Nwg1RejectionV1.AUTHORITY_MISMATCH,
                Nwg1ValidationStageV1.BINDING_SEMANTICS);

        var binding = kafka.sealed().directory().bindings().getFirst();
        byte[] wrongBindingId = binding.bindingId();
        wrongBindingId[0] ^= 1;
        var wrongBinding = new Nwg1DirectoryV1.BindingContext(
                wrongBindingId,
                binding.storageEpochId(),
                binding.ownerFenceCommitment(),
                binding.nti1Bytes(),
                binding.ownerFenceKind(),
                binding.ownerFenceVersion(),
                binding.positionDomainKind(),
                binding.positionDomainVersion(),
                binding.framePolicyKind(),
                binding.framePolicyVersion());
        var wrongBindingDirectory = new Nwg1DirectoryV1(
                1,
                List.of(wrongBinding),
                kafka.sealed().directory().appendUnits(),
                kafka.sealed().directory().frames());
        byte[] wrongBindingBody = resignDirectory(kafka, wrongBindingDirectory);
        assertReadRejected(
                wrongBindingBody,
                Nwg1CommitmentsV1.sha256(wrongBindingBody),
                kafka.verificationContext(),
                Nwg1RejectionV1.AUTHORITY_MISMATCH,
                Nwg1ValidationStageV1.BINDING_SEMANTICS);

        var unit = (Nwg1DirectoryV1.KafkaAppendUnit)
                kafka.sealed().directory().appendUnits().getFirst();
        var wrongDigestUnit = new Nwg1DirectoryV1.KafkaAppendUnit(
                unit.contextOrdinal(),
                unit.firstFrameOrdinal(),
                unit.frameCount(),
                unit.partitionId(),
                unit.kafkaLeaderEpoch(),
                unit.startOffset(),
                unit.endOffsetExclusive(),
                unit.appendCommitSetId(),
                unit.storageAttemptId(),
                new byte[32]);
        var wrongDigestDirectory = new Nwg1DirectoryV1(
                1,
                kafka.sealed().directory().bindings(),
                List.of(wrongDigestUnit),
                kafka.sealed().directory().frames());
        byte[] wrongDigestBody = resignDirectory(kafka, wrongDigestDirectory);
        assertReadRejected(
                wrongDigestBody,
                Nwg1CommitmentsV1.sha256(wrongDigestBody),
                kafka.verificationContext(),
                Nwg1RejectionV1.DIGEST_MISMATCH,
                Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS);

        byte[] corruptNative = kafka.plan().frames().getFirst().decodedPayload();
        corruptNative[30] ^= 1;
        var corruptFrame = new GroupEncodingPlanV1.PlannedFrame(0, corruptNative, corruptNative, 100, 101, 0, 0);
        var corruptUnit = new Nwg1DirectoryV1.KafkaAppendUnit(
                0,
                0,
                1,
                unit.partitionId(),
                unit.kafkaLeaderEpoch(),
                100,
                101,
                unit.appendCommitSetId(),
                unit.storageAttemptId(),
                Nwg1CommitmentsV1.sha256(corruptNative));
        GroupEncodingPlanV1 corruptPlan = copyPlan(kafka.plan(), List.of(corruptUnit), List.of(corruptFrame));
        Nwg1SealedObjectV1 corruptSealed = Nwg1ObjectWriterV1.sealEncodedPlan(
                corruptPlan, kafka.sealed().header().laneSequence(), Nwg1GoldenCorpusV1.WAL_RUN_KEY);
        assertReadRejected(
                corruptSealed.body(),
                corruptSealed.bodySha256(),
                kafka.verificationContext(),
                Nwg1RejectionV1.NATIVE_CHECKSUM_MISMATCH,
                Nwg1ValidationStageV1.NATIVE_FRAME);

        var pulsar = Nwg1GoldenCorpusV1.vectors().get(3);
        var wrongSlice = new Nwg1VerificationContextV1(
                pulsar.verificationContext().protocolCell(),
                pulsar.verificationContext().cellProviderScopeId(),
                pulsar.verificationContext().walRunRootSha256(),
                pulsar.verificationContext().envelope(),
                pulsar.verificationContext().ownerWitnessProvider(),
                new Nwg1StrictNativePayloadVerifierV1(),
                1L << 40,
                2L << 40);
        assertReadRejected(
                pulsar.sealed().body(),
                pulsar.sealed().bodySha256(),
                wrongSlice,
                Nwg1RejectionV1.VALUE_DOMAIN_VIOLATION,
                Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS);
    }

    @Test
    void readerRejectsStorageEpochAndEveryClosedBindingPolicyFamily() {
        var vector = Nwg1GoldenCorpusV1.vectors().getFirst();
        var binding = vector.sealed().directory().bindings().getFirst();
        byte[] wrongEpoch = binding.storageEpochId();
        wrongEpoch[0] ^= 1;
        List<Nwg1DirectoryV1.BindingContext> invalidBindings = List.of(
                bindingWith(binding, wrongEpoch, binding.ownerFenceKind(), binding.positionDomainKind(), 1),
                bindingWith(binding, binding.storageEpochId(), 99, binding.positionDomainKind(), 1),
                bindingWith(binding, binding.storageEpochId(), binding.ownerFenceKind(), 99, 1),
                bindingWith(
                        binding, binding.storageEpochId(), binding.ownerFenceKind(), binding.positionDomainKind(), 99));
        for (int ordinal = 0; ordinal < invalidBindings.size(); ordinal++) {
            var directory = new Nwg1DirectoryV1(
                    1,
                    List.of(invalidBindings.get(ordinal)),
                    vector.sealed().directory().appendUnits(),
                    vector.sealed().directory().frames());
            byte[] body = resignDirectory(vector, directory);
            assertReadRejected(
                    body,
                    Nwg1CommitmentsV1.sha256(body),
                    vector.verificationContext(),
                    ordinal == 0 ? Nwg1RejectionV1.AUTHORITY_MISMATCH : Nwg1RejectionV1.UNKNOWN_CODE,
                    Nwg1ValidationStageV1.BINDING_SEMANTICS);
        }
    }

    @Test
    void writerOrReaderRejectsFrameOwnershipKafkaGapOverlapUnionAndPulsarCoverage() {
        var multi = Nwg1GoldenCorpusV1.vectors().get(1);
        List<GroupEncodingPlanV1.PlannedFrame> baseFrames = multi.plan().frames();
        List<GroupEncodingPlanV1.PlannedFrame> gapFrames =
                List.of(baseFrames.get(0), frameWith(baseFrames.get(1), 0, 202, 203), baseFrames.get(2));
        assertSealedPlanRejected(
                multi, copyPlan(multi.plan(), multi.plan().appendUnits(), gapFrames), Nwg1RejectionV1.RANGE_GAP);
        List<GroupEncodingPlanV1.PlannedFrame> overlapFrames =
                List.of(baseFrames.get(0), frameWith(baseFrames.get(1), 0, 200, 201), baseFrames.get(2));
        assertSealedPlanRejected(
                multi,
                copyPlan(multi.plan(), multi.plan().appendUnits(), overlapFrames),
                Nwg1RejectionV1.RANGE_OVERLAP);

        var single = Nwg1GoldenCorpusV1.vectors().getFirst();
        var singleUnit =
                (Nwg1DirectoryV1.KafkaAppendUnit) single.plan().appendUnits().getFirst();
        var wrongUnion = new Nwg1DirectoryV1.KafkaAppendUnit(
                singleUnit.contextOrdinal(),
                singleUnit.firstFrameOrdinal(),
                singleUnit.frameCount(),
                singleUnit.partitionId(),
                singleUnit.kafkaLeaderEpoch(),
                singleUnit.startOffset(),
                singleUnit.endOffsetExclusive() + 1,
                singleUnit.appendCommitSetId(),
                singleUnit.storageAttemptId(),
                singleUnit.assignedPayloadSha256());
        assertSealedPlanRejected(
                single,
                copyPlan(single.plan(), List.of(wrongUnion), single.plan().frames()),
                Nwg1RejectionV1.COVERAGE_MISMATCH);

        List<GroupEncodingPlanV1.PlannedFrame> wrongOwnerFrames = List.of(
                baseFrames.get(0),
                frameWith(
                        baseFrames.get(1),
                        1,
                        baseFrames.get(1).coverage0(),
                        baseFrames.get(1).coverage1()),
                baseFrames.get(2));
        assertThatThrownBy(() -> copyPlan(multi.plan(), multi.plan().appendUnits(), wrongOwnerFrames))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dense append-unit range");

        var pulsar = Nwg1GoldenCorpusV1.vectors().get(3);
        var pulsarFrame = pulsar.plan().frames().getFirst();
        var wrongPulsarCoverage = frameWith(
                pulsarFrame, pulsarFrame.appendUnitOrdinal(), pulsarFrame.coverage0() + 1, pulsarFrame.coverage1());
        assertSealedPlanRejected(
                pulsar,
                copyPlan(pulsar.plan(), pulsar.plan().appendUnits(), List.of(wrongPulsarCoverage)),
                Nwg1RejectionV1.COVERAGE_MISMATCH);
    }

    @Test
    void stagedVerifierConsumesRootLeafDigestEnvelopeAndExactlyOneUnwrap() {
        for (var vector : Nwg1GoldenCorpusV1.vectors()) {
            for (Nwg1VerificationPathV1 path : Nwg1VerificationPathV1.values()) {
                AtomicInteger unwraps = new AtomicInteger();
                var root = new Nwg1RootAuthorityV1(
                        vector.verificationContext().exactNpc1(),
                        Nwg1CommitmentsV1.protocolCell(
                                vector.verificationContext().exactNpc1()),
                        vector.verificationContext().cellProviderScopeId(),
                        vector.verificationContext().walRunRootSha256(),
                        vector.verificationContext().envelope().framedBytes(),
                        Nwg1CommitmentsV1.wrappedEnvelope(
                                vector.verificationContext().envelope()));
                var request = new Nwg1ObjectVerifierV1.Request(
                        path,
                        root,
                        vector.verificationContext(),
                        vector.sealed().leafUtf8().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        vector.sealed().body(),
                        0,
                        envelope -> {
                            unwraps.incrementAndGet();
                            return Nwg1GoldenCorpusV1.WAL_RUN_KEY.clone();
                        });
                int expectedFrames = path == Nwg1VerificationPathV1.ROUTINE_RANGE_READ
                        ? 1
                        : vector.plan().frames().size();
                assertThat(Nwg1ObjectVerifierV1.verify(request).decodedFrames()).hasSize(expectedFrames);
                assertThat(unwraps).hasValue(1);
            }
        }
    }

    @Test
    void stagedVerifierRejectsAtRootLeafBodyAndEnvelopeBeforeReadingDirectory() {
        var vector = Nwg1GoldenCorpusV1.vectors().getFirst();
        var baseRoot = new Nwg1RootAuthorityV1(
                vector.verificationContext().exactNpc1(),
                Nwg1CommitmentsV1.protocolCell(vector.verificationContext().exactNpc1()),
                vector.verificationContext().cellProviderScopeId(),
                vector.verificationContext().walRunRootSha256(),
                vector.verificationContext().envelope().framedBytes(),
                Nwg1CommitmentsV1.wrappedEnvelope(vector.verificationContext().envelope()));
        byte[] wrongScope = baseRoot.cellProviderScopeId();
        wrongScope[0] ^= 1;
        assertVerifyRejected(
                request(vector, Nwg1VerificationPathV1.OPEN_RUN_RECOVERY, rootWithScope(baseRoot, wrongScope)),
                Nwg1RejectionV1.AUTHORITY_MISMATCH,
                Nwg1ValidationStageV1.ROOT_AUTHORITY);

        byte[] badLeaf = vector.sealed().leafUtf8().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        badLeaf[badLeaf.length - 1] = 'x';
        assertVerifyRejected(
                request(
                        vector,
                        Nwg1VerificationPathV1.OPEN_RUN_RECOVERY,
                        baseRoot,
                        badLeaf,
                        vector.sealed().body()),
                Nwg1RejectionV1.NON_CANONICAL_ENCODING,
                Nwg1ValidationStageV1.LEAF);

        byte[] badBody = vector.sealed().body();
        badBody[badBody.length - 1] ^= 1;
        assertVerifyRejected(
                request(
                        vector,
                        Nwg1VerificationPathV1.FULL_BODY_RECONCILIATION,
                        baseRoot,
                        vector.sealed().leafUtf8().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        badBody),
                Nwg1RejectionV1.DIGEST_MISMATCH,
                Nwg1ValidationStageV1.OBJECT_BODY_DIGEST);

        byte[] trailingEnvelope = baseRoot.framedEnvelope();
        trailingEnvelope = Arrays.copyOf(trailingEnvelope, trailingEnvelope.length + 1);
        ByteBuffer.wrap(trailingEnvelope).order(ByteOrder.BIG_ENDIAN).putInt(4, trailingEnvelope.length - 8);
        var trailingRoot = new Nwg1RootAuthorityV1(
                baseRoot.exactNpc1(),
                baseRoot.protocolCellCommitment(),
                baseRoot.cellProviderScopeId(),
                baseRoot.walRunRootSha256(),
                trailingEnvelope,
                baseRoot.wrappedEnvelopeCommitment());
        assertVerifyRejected(
                request(vector, Nwg1VerificationPathV1.OPEN_RUN_RECOVERY, trailingRoot),
                Nwg1RejectionV1.TRAILING_BYTES,
                Nwg1ValidationStageV1.KMS_ENVELOPE);
    }

    @Test
    void routineRangeReadValidatesOnlySelectedFrameAndOmitsWholeBodyDigest() {
        var vector = Nwg1GoldenCorpusV1.vectors().get(1);
        var secondFrame = vector.sealed().directory().frames().get(1);
        byte[] body = vector.sealed().body();
        body[Math.toIntExact(secondFrame.storedBodyOffset())] ^= 1;
        var root = new Nwg1RootAuthorityV1(
                vector.verificationContext().exactNpc1(),
                Nwg1CommitmentsV1.protocolCell(vector.verificationContext().exactNpc1()),
                vector.verificationContext().cellProviderScopeId(),
                vector.verificationContext().walRunRootSha256(),
                vector.verificationContext().envelope().framedBytes(),
                Nwg1CommitmentsV1.wrappedEnvelope(vector.verificationContext().envelope()));
        var routine = request(
                vector,
                Nwg1VerificationPathV1.ROUTINE_RANGE_READ,
                root,
                vector.sealed().leafUtf8().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                body);
        assertThat(Nwg1ObjectVerifierV1.verify(routine).decodedFrames()).hasSize(1);
        assertVerifyRejected(
                request(
                        vector,
                        Nwg1VerificationPathV1.FULL_BODY_RECONCILIATION,
                        root,
                        vector.sealed().leafUtf8().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        body),
                Nwg1RejectionV1.DIGEST_MISMATCH,
                Nwg1ValidationStageV1.OBJECT_BODY_DIGEST);
    }

    private record StreamingFixture(
            Nwg1GoldenCorpusV1.Vector authority, GroupEncodingPlanV1 plan, Nwg1SealedObjectV1 sealed) {}

    private static StreamingFixture highCompressionStreamingFixture() {
        Nwg1GoldenCorpusV1.Vector authority = Nwg1GoldenCorpusV1.vectors().stream()
                .filter(vector -> vector.id().equals("NWG1_KAFKA_FIXED_ZSTD_V1"))
                .findFirst()
                .orElseThrow();
        Nwg1DirectoryV1.KafkaAppendUnit authorityUnit =
                (Nwg1DirectoryV1.KafkaAppendUnit) authority.plan().appendUnits().getFirst();
        byte[] firstPayload = Nwg1GoldenCorpusV1.kafkaBatch(64 * 1024, 900, 11);
        byte[] secondPayload = Nwg1GoldenCorpusV1.kafkaBatch(64 * 1024, 901, 12);
        Nwg1ZstdV1.EncodingResult firstEncoding = Nwg1ZstdV1.encodeIfSmaller(firstPayload);
        Nwg1ZstdV1.EncodingResult secondEncoding = Nwg1ZstdV1.encodeIfSmaller(secondPayload);
        assertThat(firstEncoding.codecKind()).isEqualTo(Nwg1ConstantsV1.CODEC_ZSTD_KIND);
        assertThat(secondEncoding.codecKind()).isEqualTo(Nwg1ConstantsV1.CODEC_ZSTD_KIND);
        assertThat(firstEncoding.preAeadBytes().length).isLessThan(firstPayload.length / 16);
        assertThat(secondEncoding.preAeadBytes().length).isLessThan(secondPayload.length / 16);
        List<GroupEncodingPlanV1.PlannedFrame> frames = List.of(
                new GroupEncodingPlanV1.PlannedFrame(
                        0,
                        firstPayload,
                        firstEncoding.preAeadBytes(),
                        900,
                        901,
                        firstEncoding.codecKind(),
                        firstEncoding.codecVersion()),
                new GroupEncodingPlanV1.PlannedFrame(
                        0,
                        secondPayload,
                        secondEncoding.preAeadBytes(),
                        901,
                        902,
                        secondEncoding.codecKind(),
                        secondEncoding.codecVersion()));
        Nwg1DirectoryV1.KafkaAppendUnit unit = new Nwg1DirectoryV1.KafkaAppendUnit(
                authorityUnit.contextOrdinal(),
                0,
                2,
                authorityUnit.partitionId(),
                authorityUnit.kafkaLeaderEpoch(),
                900,
                902,
                authorityUnit.appendCommitSetId(),
                authorityUnit.storageAttemptId(),
                assignedDigest(frames, 0));
        GroupEncodingPlanV1 source = authority.plan();
        GroupEncodingPlanV1 plan = new GroupEncodingPlanV1(
                source.protocolKind(),
                source.shardId(),
                source.shardRunEpoch(),
                source.laneId(),
                source.packingPolicyVersion(),
                Math.addExact(firstPayload.length, secondPayload.length),
                source.resolvedLingerNanos(),
                source.actualCloseLingerNanos(),
                source.closeReason(),
                source.protocolCellCommitment(),
                source.providerScopeId(),
                source.rootSha256(),
                source.envelopeCommitment(),
                source.bindings(),
                List.of(unit),
                frames);
        Nwg1SealedObjectV1 sealed = Nwg1ObjectWriterV1.seal(
                plan,
                authority.sealed().header().laneSequence(),
                authority.walRunKey(),
                authority.verificationContext());
        return new StreamingFixture(authority, plan, sealed);
    }

    private static Nwg1ObjectReaderV1.AuthenticatedPrefix authenticatedPrefix(StreamingFixture fixture, byte[] body) {
        int prefixEnd = Math.toIntExact(fixture.sealed().header().directoryPrefixEnd());
        return Nwg1ObjectReaderV1.readAuthenticatedPrefix(
                Arrays.copyOf(body, prefixEnd),
                body.length,
                fixture.authority().verificationContext(),
                fixture.authority().walRunKey());
    }

    private static void assertStreamingRejected(
            StreamingFixture fixture, byte[] body, Nwg1RejectionV1 expectedRejection) {
        Nwg1ObjectReaderV1.AuthenticatedPrefix prefix = authenticatedPrefix(fixture, body);
        AtomicInteger consumed = new AtomicInteger();
        assertThatThrownBy(() -> Nwg1ObjectReaderV1.readSelectedAppendUnitStreaming(
                        prefix,
                        (range, absoluteFrameOrdinal) -> Arrays.copyOfRange(
                                body, Math.toIntExact(range.inclusiveStart()), Math.toIntExact(range.exclusiveEnd())),
                        0,
                        fixture.authority().verificationContext(),
                        fixture.authority().walRunKey(),
                        (frame, payload) -> consumed.incrementAndGet()))
                .isInstanceOfSatisfying(Nwg1ValidationException.class, failure -> {
                    assertThat(failure.rejection()).isEqualTo(expectedRejection);
                    assertThat(failure.stage()).isEqualTo(Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS);
                });
        assertThat(consumed).hasValue(2);
    }

    private static void assertErased(byte[] value) {
        assertThat(value).isNotNull().containsOnly((byte) 0);
    }

    private static void assertErased(ByteBuffer value) {
        assertThat(value).isNotNull();
        ByteBuffer exact = value.duplicate();
        exact.position(0);
        byte[] bytes = new byte[exact.remaining()];
        exact.get(bytes);
        assertErased(bytes);
    }

    private static GroupEncodingPlanV1 copyPlan(
            GroupEncodingPlanV1 source,
            List<? extends Nwg1DirectoryV1.AppendUnit> units,
            List<GroupEncodingPlanV1.PlannedFrame> frames) {
        return new GroupEncodingPlanV1(
                source.protocolKind(),
                source.shardId(),
                source.shardRunEpoch(),
                source.laneId(),
                source.packingPolicyVersion(),
                source.resolvedTargetBytes(),
                source.resolvedLingerNanos(),
                source.actualCloseLingerNanos(),
                source.closeReason(),
                source.protocolCellCommitment(),
                source.providerScopeId(),
                source.rootSha256(),
                source.envelopeCommitment(),
                source.bindings(),
                units,
                frames);
    }

    private static Nwg1DirectoryV1.BindingContext bindingWith(
            Nwg1DirectoryV1.BindingContext source,
            byte[] storageEpochId,
            int ownerFenceKind,
            int positionDomainKind,
            int framePolicyKind) {
        return new Nwg1DirectoryV1.BindingContext(
                source.bindingId(),
                storageEpochId,
                source.ownerFenceCommitment(),
                source.nti1Bytes(),
                ownerFenceKind,
                source.ownerFenceVersion(),
                positionDomainKind,
                source.positionDomainVersion(),
                framePolicyKind,
                source.framePolicyVersion());
    }

    private static GroupEncodingPlanV1.PlannedFrame frameWith(
            GroupEncodingPlanV1.PlannedFrame source, long unitOrdinal, long coverage0, long coverage1) {
        return new GroupEncodingPlanV1.PlannedFrame(
                unitOrdinal,
                source.decodedPayload(),
                source.preAeadBytes(),
                coverage0,
                coverage1,
                source.actualCodecKind(),
                source.actualCodecVersion());
    }

    private static Nwg1DirectoryV1.KafkaAppendUnit kafkaUnitWithDigest(
            Nwg1DirectoryV1.KafkaAppendUnit source, byte[] assignedDigest) {
        return new Nwg1DirectoryV1.KafkaAppendUnit(
                source.contextOrdinal(),
                source.firstFrameOrdinal(),
                source.frameCount(),
                source.partitionId(),
                source.kafkaLeaderEpoch(),
                source.startOffset(),
                source.endOffsetExclusive(),
                source.appendCommitSetId(),
                source.storageAttemptId(),
                assignedDigest);
    }

    private static byte[] assignedDigest(List<GroupEncodingPlanV1.PlannedFrame> frames, long unitOrdinal) {
        ByteArrayOutputStream assigned = new ByteArrayOutputStream();
        for (GroupEncodingPlanV1.PlannedFrame frame : frames) {
            if (frame.appendUnitOrdinal() == unitOrdinal) {
                assigned.writeBytes(frame.decodedPayload());
            }
        }
        return Nwg1CommitmentsV1.sha256(assigned.toByteArray());
    }

    private static void assertSealedPlanRejected(
            Nwg1GoldenCorpusV1.Vector vector, GroupEncodingPlanV1 plan, Nwg1RejectionV1 rejection) {
        Nwg1SealedObjectV1 sealed = Nwg1ObjectWriterV1.sealEncodedPlan(
                plan, vector.sealed().header().laneSequence(), vector.walRunKey());
        assertReadRejected(
                sealed.body(),
                sealed.bodySha256(),
                vector.verificationContext(),
                rejection,
                Nwg1ValidationStageV1.APPEND_UNIT_SEMANTICS);
    }

    private static byte[] resignDirectory(Nwg1GoldenCorpusV1.Vector vector, Nwg1DirectoryV1 replacement) {
        byte[] oldBody = vector.sealed().body();
        byte[] header = Arrays.copyOfRange(oldBody, 0, 256);
        byte[] info = Nwg1CryptoV1.objectKeyInfo(
                vector.plan().shardId(),
                vector.plan().shardRunEpoch(),
                vector.plan().laneId(),
                vector.sealed().header().laneSequence());
        byte[] key = Nwg1CryptoV1.deriveObjectAeadKey(
                Nwg1GoldenCorpusV1.WAL_RUN_KEY, vector.plan().rootSha256(), info);
        byte[] encrypted = Nwg1CryptoV1.encrypt(
                key,
                Nwg1CryptoV1.directoryNonce(),
                Nwg1CryptoV1.directoryAad(header),
                Nwg1DirectoryCodecV1.encode(replacement));
        ByteArrayOutputStream body = new ByteArrayOutputStream(oldBody.length);
        body.writeBytes(header);
        body.writeBytes(encrypted);
        body.writeBytes(Arrays.copyOfRange(
                oldBody, Math.toIntExact(vector.sealed().header().directoryPrefixEnd()), oldBody.length));
        Arrays.fill(key, (byte) 0);
        Arrays.fill(info, (byte) 0);
        return body.toByteArray();
    }

    private static byte[] resignDirectory(StreamingFixture fixture, Nwg1DirectoryV1 replacement) {
        byte[] oldBody = fixture.sealed().body();
        byte[] header = Arrays.copyOfRange(oldBody, 0, Nwg1ConstantsV1.HEADER_BYTES);
        byte[] info = Nwg1CryptoV1.objectKeyInfo(
                fixture.plan().shardId(),
                fixture.plan().shardRunEpoch(),
                fixture.plan().laneId(),
                fixture.sealed().header().laneSequence());
        byte[] key = Nwg1CryptoV1.deriveObjectAeadKey(
                fixture.authority().walRunKey(), fixture.plan().rootSha256(), info);
        try {
            byte[] encrypted = Nwg1CryptoV1.encrypt(
                    key,
                    Nwg1CryptoV1.directoryNonce(),
                    Nwg1CryptoV1.directoryAad(header),
                    Nwg1DirectoryCodecV1.encode(replacement));
            ByteArrayOutputStream body = new ByteArrayOutputStream(oldBody.length);
            body.writeBytes(header);
            body.writeBytes(encrypted);
            body.writeBytes(Arrays.copyOfRange(
                    oldBody, Math.toIntExact(fixture.sealed().header().directoryPrefixEnd()), oldBody.length));
            return body.toByteArray();
        } finally {
            Arrays.fill(key, (byte) 0);
            Arrays.fill(info, (byte) 0);
        }
    }

    private static void assertReadRejected(
            byte[] body,
            byte[] sha,
            Nwg1VerificationContextV1 context,
            Nwg1RejectionV1 rejection,
            Nwg1ValidationStageV1 stage) {
        assertThatThrownBy(() -> Nwg1ObjectReaderV1.read(body, sha, context, Nwg1GoldenCorpusV1.WAL_RUN_KEY))
                .isInstanceOfSatisfying(Nwg1ValidationException.class, failure -> {
                    assertThat(failure.rejection()).isEqualTo(rejection);
                    assertThat(failure.stage()).isEqualTo(stage);
                });
    }

    private static Nwg1ObjectVerifierV1.Request request(
            Nwg1GoldenCorpusV1.Vector vector, Nwg1VerificationPathV1 path, Nwg1RootAuthorityV1 root) {
        return request(
                vector,
                path,
                root,
                vector.sealed().leafUtf8().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                vector.sealed().body());
    }

    private static Nwg1ObjectVerifierV1.Request request(
            Nwg1GoldenCorpusV1.Vector vector,
            Nwg1VerificationPathV1 path,
            Nwg1RootAuthorityV1 root,
            byte[] leaf,
            byte[] body) {
        return new Nwg1ObjectVerifierV1.Request(
                path,
                root,
                vector.verificationContext(),
                leaf,
                body,
                0,
                envelope -> Nwg1GoldenCorpusV1.WAL_RUN_KEY.clone());
    }

    private static Nwg1RootAuthorityV1 rootWithScope(Nwg1RootAuthorityV1 root, byte[] scope) {
        return new Nwg1RootAuthorityV1(
                root.exactNpc1(),
                root.protocolCellCommitment(),
                scope,
                root.walRunRootSha256(),
                root.framedEnvelope(),
                root.wrappedEnvelopeCommitment());
    }

    private static void assertVerifyRejected(
            Nwg1ObjectVerifierV1.Request request, Nwg1RejectionV1 rejection, Nwg1ValidationStageV1 stage) {
        assertThatThrownBy(() -> Nwg1ObjectVerifierV1.verify(request))
                .isInstanceOfSatisfying(Nwg1ValidationException.class, failure -> {
                    assertThat(failure.rejection()).isEqualTo(rejection);
                    assertThat(failure.stage()).isEqualTo(stage);
                });
    }

    private static void assertCodecRejected(byte[] bytes, int decodedLength) {
        assertThatThrownBy(() -> Nwg1ZstdV1.validateStandardFrame(bytes, decodedLength))
                .isInstanceOfSatisfying(Nwg1ValidationException.class, failure -> {
                    assertThat(failure.rejection()).isEqualTo(Nwg1RejectionV1.CODEC_CONTRACT_VIOLATION);
                    assertThat(failure.stage()).isEqualTo(Nwg1ValidationStageV1.FRAME_CODEC);
                });
    }

    private static byte[] independentStreamingDecode(byte[] frame) throws IOException {
        try (var input = new ZstdInputStream(new ByteArrayInputStream(frame))) {
            return input.readAllBytes();
        }
    }
}
