/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeLimits;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Strict, closed, canonical codecs for the production Root/Pointer/Seal/checkpoint control records. */
public final class WalRunControlCodec {
    private static final int ROOT_MAGIC = 0x4e575231; // NWR1
    private static final int POINTER_MAGIC = 0x4e575031; // NWP1
    private static final int SEAL_MAGIC = 0x4e575331; // NWS1
    private static final int PAGE_MAGIC = 0x4e574331; // NWC1
    private static final int CHECKPOINT_HEAD_MAGIC = 0x4e574831; // NWH1
    private static final int FORMAT_VERSION = 1;
    private static final int MAX_CONTROL_RECORD_BYTES = 1024 * 1024;

    private WalRunControlCodec() {}

    private static void writeCheckpointRow(DataOutputStream out, ProviderResolvedExtentRowV1 row) throws IOException {
        out.writeByte(row.laneId().code());
        out.writeLong(row.laneSequence());
        out.writeInt(row.directoryPrefixEnd());
        out.writeLong(row.bodyLength());
        writeDigest(out, row.objectSha256());
        out.writeByte(row.providerProof().mode().code());
        writeBytes(out, row.providerProof().canonicalVersionToken(), 65_535);
    }

    public static CanonicalBytes encodeRoot(WalRunRootRecord root) {
        return encode(out -> {
            writePreamble(out, ROOT_MAGIC);
            out.writeInt(root.shardId());
            out.writeLong(root.shardRunEpoch());
            out.writeLong(root.walRunSessionId().highBits());
            out.writeLong(root.walRunSessionId().lowBits());
            out.writeLong(root.openedAtMillis());
            writeBytes(out, ProtocolCellIdentityCodecV1.encode(root.protocolCellIdentity()), 64);
            out.write(root.providerScopeId().digest().bytes().toByteArray());
            writeFormatContract(out, root.formatContract());
            writeNwg1AdmissionCaps(out, root.nwg1AdmissionCaps());
            WalRunBounds bounds = root.bounds();
            out.writeLong(bounds.maxExtentCount());
            out.writeLong(bounds.maxCanonicalBodyBytes());
            out.writeLong(bounds.maxRunAgeMillis());
            out.writeInt(bounds.maxRecoverablePredecessorRuns());
            WalCheckpointPolicy policy = root.checkpointPolicy();
            out.writeLong(policy.proactiveCadenceMillis());
            out.writeInt(policy.maxUncheckpointedExtents());
            out.writeLong(policy.maxUncheckpointedBytes());
            out.writeLong(policy.maxUncheckpointedAgeMillis());
            out.writeInt(policy.maxRowsPerPage());
            out.writeInt(policy.maxCanonicalPageBytes());
            ObjectProviderRootConfiguration provider = root.providerConfiguration();
            out.writeByte(provider.accessProfile().code());
            writeUtf8(out, provider.adapterVersion(), 128);
            writeUtf8(out, provider.canonicalizerVersion(), 128);
            writeUtf8(out, provider.exclusiveNamespacePrefix(), 512);
            out.writeByte(provider.proofMode().code());
            out.writeShort(provider.proofTokenHardCap());
            out.writeLong(provider.maxObjectBodyBytes());
            out.writeLong(provider.maxSinglePutBytes());
            out.writeInt(provider.maxSingleRangeReadBytes());
            out.writeInt(provider.maxPrefixSegmentsPerExtent());
            out.writeInt(provider.maxListPageKeys());
            writeDigest(out, provider.capabilityReceiptSha256());
            writeRecoveryEnvelope(out, root.recoveryEnvelope());
            WrappedRunKeyEnvelope envelope = root.wrappedRunKey();
            out.write(envelope.framedBytes().toByteArray());
            writeOptionalFlag(out, root.predecessor().isPresent());
            if (root.predecessor().isPresent()) {
                WalRunPredecessor predecessor = root.predecessor().orElseThrow();
                writeReference(out, predecessor.root());
                writeUtf8(out, predecessor.sealKey(), WalRunReference.MAX_METADATA_KEY_BYTES);
                writeDigest(out, predecessor.sealSha256());
                writeOptionalFlag(out, predecessor.terminalProtocolCheckpoint().isPresent());
                if (predecessor.terminalProtocolCheckpoint().isPresent()) {
                    TerminalProtocolCheckpointBindingV1 binding =
                            predecessor.terminalProtocolCheckpoint().orElseThrow();
                    out.writeByte(binding.protocolKind().code());
                    writeUtf8(out, binding.terminalHeadKey(), WalRunReference.MAX_METADATA_KEY_BYTES);
                    writeDigest(out, binding.terminalHeadValueSha256());
                }
            }
        });
    }

    public static WalRunRootRecord decodeRoot(CanonicalBytes encoded) {
        WalRunRootRecord decoded = decode(encoded, input -> {
            readPreamble(input, ROOT_MAGIC);
            int shardId = input.readInt();
            long runEpoch = input.readLong();
            Id128 runSessionId = new Id128(input.readLong(), input.readLong());
            long openedAtMillis = input.readLong();
            ProtocolCellIdentity protocolCellIdentity =
                    ProtocolCellIdentityCodecV1.decode(readBytes(input, 64).toByteArray());
            CellProviderScopeId scopeId = new CellProviderScopeId(readDigest(input));
            WalRunFormatContractV1 formatContract = readFormatContract(input);
            Nwg1RootAdmissionCaps nwg1Caps = readNwg1AdmissionCaps(input);
            WalRunBounds bounds =
                    new WalRunBounds(input.readLong(), input.readLong(), input.readLong(), input.readInt());
            WalCheckpointPolicy policy = new WalCheckpointPolicy(
                    input.readLong(),
                    input.readInt(),
                    input.readLong(),
                    input.readLong(),
                    input.readInt(),
                    input.readInt());
            ObjectProviderRootConfiguration provider = new ObjectProviderRootConfiguration(
                    ObjectProviderAccessProfile.fromCode(input.readUnsignedByte()),
                    readUtf8(input, 128),
                    readUtf8(input, 128),
                    readUtf8(input, 512),
                    ProviderProofMode.fromCode(input.readUnsignedByte()),
                    input.readUnsignedShort(),
                    input.readLong(),
                    input.readLong(),
                    input.readInt(),
                    input.readInt(),
                    input.readInt(),
                    readDigest(input));
            RecoveryEnvelopeLimits recoveryEnvelope = readRecoveryEnvelope(input);
            WrappedRunKeyEnvelope envelope = readWrappedRunKeyEnvelope(input);
            Optional<WalRunPredecessor> predecessor =
                    readOptionalFlag(input) ? Optional.of(readPredecessor(input)) : Optional.empty();
            return new WalRunRootRecord(
                    shardId,
                    runEpoch,
                    runSessionId,
                    openedAtMillis,
                    protocolCellIdentity,
                    scopeId,
                    formatContract,
                    nwg1Caps,
                    bounds,
                    policy,
                    provider,
                    recoveryEnvelope,
                    envelope,
                    predecessor);
        });
        requireCanonical(encoded, encodeRoot(decoded));
        return decoded;
    }

    public static Sha256Digest rootSha256(WalRunRootRecord root) {
        return Sha256Digest.hash(encodeRoot(root));
    }

    public static CanonicalBytes encodePointer(CurrentWalRunPointer pointer) {
        return encode(out -> {
            writePreamble(out, POINTER_MAGIC);
            writeReference(out, pointer.current());
        });
    }

    public static CurrentWalRunPointer decodePointer(CanonicalBytes encoded) {
        CurrentWalRunPointer decoded = decode(encoded, input -> {
            readPreamble(input, POINTER_MAGIC);
            return new CurrentWalRunPointer(readReference(input));
        });
        requireCanonical(encoded, encodePointer(decoded));
        return decoded;
    }

    public static CanonicalBytes encodeSeal(WalRunSealRecord seal) {
        return encode(out -> {
            writePreamble(out, SEAL_MAGIC);
            writeReference(out, seal.root());
            writeVector(out, seal.terminalSequence());
            writeUtf8(out, seal.finalCheckpointHeadKey(), WalRunReference.MAX_METADATA_KEY_BYTES);
            writeDigest(out, seal.finalCheckpointHeadSha256());
            out.writeLong(seal.aggregateExtentCount());
            out.writeLong(seal.aggregateCanonicalBodyBytes());
        });
    }

    public static WalRunSealRecord decodeSeal(CanonicalBytes encoded) {
        WalRunSealRecord decoded = decode(encoded, input -> {
            readPreamble(input, SEAL_MAGIC);
            return new WalRunSealRecord(
                    readReference(input),
                    readVector(input),
                    readUtf8(input, WalRunReference.MAX_METADATA_KEY_BYTES),
                    readDigest(input),
                    input.readLong(),
                    input.readLong());
        });
        requireCanonical(encoded, encodeSeal(decoded));
        return decoded;
    }

    public static Sha256Digest sealSha256(WalRunSealRecord seal) {
        return Sha256Digest.hash(encodeSeal(seal));
    }

    public static CanonicalBytes encodeCheckpointPage(WalRunCheckpointPageV1 page) {
        CanonicalBytes encoded = encode(out -> {
            writePreamble(out, PAGE_MAGIC);
            writeDigest(out, page.rootSha256());
            out.writeLong(page.pageOrdinal());
            writeOptionalFlag(out, page.predecessorPageSha256().isPresent());
            if (page.predecessorPageSha256().isPresent()) {
                writeDigest(out, page.predecessorPageSha256().orElseThrow());
            }
            out.writeShort(page.extents().size());
            for (ProviderResolvedExtentRowV1 row : page.extents()) {
                writeCheckpointRow(out, row);
            }
            writeVector(out, page.coveredThrough());
        });
        if (encoded.length() > WalCheckpointPolicy.FORMAT_MAX_CANONICAL_PAGE_BYTES) {
            throw new IllegalArgumentException("canonical checkpoint page exceeds 64 KiB");
        }
        return encoded;
    }

    /** Exact standalone row wire used by checkpoint consumers that must not reproduce the page codec ad hoc. */
    public static CanonicalBytes encodeCheckpointRow(ProviderResolvedExtentRowV1 row) {
        return encode(out -> writeCheckpointRow(out, Objects.requireNonNull(row, "row")));
    }

    /** Exact canonical checkpoint-row length, including its closed proof-mode/token fields. */
    public static int checkpointRowCanonicalLength(ProviderResolvedExtentRowV1 row) {
        return encodeCheckpointRow(row).length();
    }

    /** Fixed production NONE row length: lane+sequence+prefix+body+SHA+proof tag+zero token length. */
    public static int proofNoneCheckpointRowCanonicalLength() {
        return 56;
    }

    /** Strict standalone checkpoint-row decoder; exact re-encoding rejects trailing or non-canonical wire. */
    public static ProviderResolvedExtentRowV1 decodeCheckpointRow(CanonicalBytes encoded) {
        Objects.requireNonNull(encoded, "encoded");
        ProviderResolvedExtentRowV1 decoded = decode(
                encoded,
                input -> new ProviderResolvedExtentRowV1(
                        WalLaneId.fromCode(input.readUnsignedByte()),
                        input.readLong(),
                        input.readInt(),
                        input.readLong(),
                        readDigest(input),
                        new ProviderVersionProof(
                                ProviderProofMode.fromCode(input.readUnsignedByte()), readBytes(input, 65_535))));
        requireCanonical(encoded, encodeCheckpointRow(decoded));
        return decoded;
    }

    public static WalRunCheckpointPageV1 decodeCheckpointPage(
            CanonicalBytes encoded, ObjectProviderRootConfiguration providerConfiguration) {
        if (encoded.length() > WalCheckpointPolicy.FORMAT_MAX_CANONICAL_PAGE_BYTES) {
            throw new IllegalArgumentException("canonical checkpoint page exceeds 64 KiB");
        }
        WalRunCheckpointPageV1 decoded = decode(encoded, input -> {
            readPreamble(input, PAGE_MAGIC);
            Sha256Digest rootSha = readDigest(input);
            long pageOrdinal = input.readLong();
            Optional<Sha256Digest> predecessor =
                    readOptionalFlag(input) ? Optional.of(readDigest(input)) : Optional.empty();
            int rowCount = input.readUnsignedShort();
            if (rowCount == 0 || rowCount > WalCheckpointPolicy.FORMAT_MAX_ROWS_PER_PAGE) {
                throw new IllegalArgumentException("checkpoint row count is outside the format bound");
            }
            ArrayList<ProviderResolvedExtentRowV1> rows = new ArrayList<>(rowCount);
            for (int index = 0; index < rowCount; index++) {
                WalLaneId lane = WalLaneId.fromCode(input.readUnsignedByte());
                long sequence = input.readLong();
                int prefixEnd = input.readInt();
                long bodyLength = input.readLong();
                Sha256Digest objectSha = readDigest(input);
                ProviderProofMode proofMode = ProviderProofMode.fromCode(input.readUnsignedByte());
                if (proofMode != providerConfiguration.proofMode()) {
                    throw new IllegalArgumentException("checkpoint proof mode differs from the Root");
                }
                CanonicalBytes token = readBytes(input, providerConfiguration.proofTokenHardCap());
                rows.add(new ProviderResolvedExtentRowV1(
                        lane, sequence, prefixEnd, bodyLength, objectSha, new ProviderVersionProof(proofMode, token)));
            }
            return new WalRunCheckpointPageV1(rootSha, pageOrdinal, predecessor, rows, readVector(input));
        });
        requireCanonical(encoded, encodeCheckpointPage(decoded));
        return decoded;
    }

    public static CanonicalBytes encodeCheckpointHead(WalCheckpointHeadV1 head) {
        return encode(out -> {
            writePreamble(out, CHECKPOINT_HEAD_MAGIC);
            writeDigest(out, head.rootSha256());
            out.writeLong(head.shardRunEpoch());
            out.writeLong(head.publisherEpoch());
            out.writeLong(head.pageOrdinal());
            writeOptionalFlag(out, head.pageKey().isPresent());
            if (head.pageKey().isPresent()) {
                writeUtf8(out, head.pageKey().orElseThrow(), WalRunReference.MAX_METADATA_KEY_BYTES);
                writeDigest(out, head.pageSha256().orElseThrow());
            }
            writeVector(out, head.coveredThrough());
        });
    }

    public static WalCheckpointHeadV1 decodeCheckpointHead(CanonicalBytes encoded) {
        WalCheckpointHeadV1 decoded = decode(encoded, input -> {
            readPreamble(input, CHECKPOINT_HEAD_MAGIC);
            Sha256Digest rootSha = readDigest(input);
            long runEpoch = input.readLong();
            long publisherEpoch = input.readLong();
            long pageOrdinal = input.readLong();
            boolean hasPage = readOptionalFlag(input);
            Optional<String> pageKey =
                    hasPage ? Optional.of(readUtf8(input, WalRunReference.MAX_METADATA_KEY_BYTES)) : Optional.empty();
            Optional<Sha256Digest> pageSha = hasPage ? Optional.of(readDigest(input)) : Optional.empty();
            return new WalCheckpointHeadV1(
                    rootSha, runEpoch, publisherEpoch, pageOrdinal, pageKey, pageSha, readVector(input));
        });
        requireCanonical(encoded, encodeCheckpointHead(decoded));
        return decoded;
    }

    private static CanonicalBytes encode(Encoder encoder) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                encoder.encode(output);
            }
            if (bytes.size() > MAX_CONTROL_RECORD_BYTES) {
                throw new IllegalArgumentException("control record exceeds the hard byte cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("unexpected in-memory encoding failure", failure);
        }
    }

    private static <T> T decode(CanonicalBytes encoded, Decoder<T> decoder) {
        if (encoded.isEmpty() || encoded.length() > MAX_CONTROL_RECORD_BYTES) {
            throw new IllegalArgumentException("control record length is outside the hard bound");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            T value = decoder.decode(input);
            if (input.read() != -1) {
                throw new IllegalArgumentException("control record has trailing bytes");
            }
            return value;
        } catch (EOFException failure) {
            throw new IllegalArgumentException("truncated control record", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("invalid control record", failure);
        }
    }

    private static void writePreamble(DataOutputStream output, int magic) throws IOException {
        output.writeInt(magic);
        output.writeByte(FORMAT_VERSION);
        output.writeByte(0);
        output.writeShort(0);
    }

    private static void writeFormatContract(DataOutputStream output, WalRunFormatContractV1 value) throws IOException {
        output.writeByte(value.nwg1ManifestVersion());
        output.writeByte(value.headerLayoutVersion());
        output.writeByte(value.directoryLayoutVersion());
        output.writeByte(value.bindingContextRowVersion());
        output.writeByte(value.kafkaAppendUnitRowVersion());
        output.writeByte(value.pulsarAppendUnitRowVersion());
        output.writeByte(value.commonFrameRowVersion());
        output.writeByte(value.bindingEpochValidationKind());
        output.writeByte(value.bindingEpochValidationVersion());
        output.writeByte(value.leafKeyGrammarVersion());
        output.writeByte(value.laneCatalogVersion());
        output.writeByte(value.planThenSequenceContractVersion());
        output.writeByte(value.packingPolicyCatalogVersion());
        output.writeByte(value.frameCodecRegistryKind());
        output.writeByte(value.frameCodecRegistryVersion());
        output.writeByte(value.allowedFrameCodecsVersion());
        output.writeByte(value.objectDigestKind());
        output.writeByte(value.objectDigestVersion());
        output.writeByte(value.payloadChecksumKind());
        output.writeByte(value.payloadChecksumVersion());
        output.writeByte(value.aeadKind());
        output.writeByte(value.aeadVersion());
        output.writeByte(value.kdfKind());
        output.writeByte(value.kdfVersion());
        output.writeByte(value.nonceLayoutVersion());
        output.writeByte(value.rootEnvelopeKind());
        output.writeByte(value.rootEnvelopeVersion());
    }

    private static WalRunFormatContractV1 readFormatContract(DataInputStream input) throws IOException {
        return new WalRunFormatContractV1(
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte(),
                input.readUnsignedByte());
    }

    private static void writeNwg1AdmissionCaps(DataOutputStream output, Nwg1RootAdmissionCaps value)
            throws IOException {
        output.writeLong(value.maxCanonicalBodyBytes());
        output.writeInt(value.maxDirectoryPrefixBytes());
        output.writeInt(value.maxDirectoryPlaintextBytes());
        output.writeInt(value.maxBindingContexts());
        output.writeInt(value.maxAppendUnits());
        output.writeInt(value.maxFrames());
        output.writeInt(value.maxDecodedFrameBytes());
        output.writeInt(value.maxStoredFrameBytes());
        output.writeLong(value.maxDecodedAppendUnitBytes());
        output.writeLong(value.maxTotalDecodedPayloadBytes());
    }

    private static Nwg1RootAdmissionCaps readNwg1AdmissionCaps(DataInputStream input) throws IOException {
        return new Nwg1RootAdmissionCaps(
                input.readLong(),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readLong(),
                input.readLong());
    }

    private static void writeRecoveryEnvelope(DataOutputStream output, RecoveryEnvelopeLimits value)
            throws IOException {
        output.writeInt(value.maxLiveRoots());
        output.writeInt(value.maxPredecessorRuns());
        output.writeInt(value.maxListPages());
        output.writeLong(value.maxListedKeys());
        output.writeLong(value.maxListedKeyBytes());
        output.writeInt(value.maxHeadRequests());
        output.writeInt(value.maxRangeGetRequests());
        output.writeInt(value.maxFullGetRequests());
        output.writeLong(value.maxCanonicalBodyBytes());
        output.writeLong(value.maxDecodedContexts());
        output.writeLong(value.maxDecodedFrames());
        output.writeLong(value.maxDecodedCommitSets());
        output.writeLong(value.maxWorkingMemoryBytes());
        output.writeInt(value.maxConcurrency());
        output.writeInt(value.maxRetryAttempts());
        output.writeLong(value.maxWallTimeNanos());
    }

    private static RecoveryEnvelopeLimits readRecoveryEnvelope(DataInputStream input) throws IOException {
        return new RecoveryEnvelopeLimits(
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readLong(),
                input.readLong(),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readLong(),
                input.readLong(),
                input.readLong(),
                input.readLong(),
                input.readLong(),
                input.readInt(),
                input.readInt(),
                input.readLong());
    }

    private static WrappedRunKeyEnvelope readWrappedRunKeyEnvelope(DataInputStream input) throws IOException {
        int kind = input.readUnsignedShort();
        int version = input.readUnsignedShort();
        long declaredLength = Integer.toUnsignedLong(input.readInt());
        if (kind != WrappedRunKeyEnvelope.KIND
                || version != WrappedRunKeyEnvelope.VERSION
                || declaredLength > WrappedRunKeyEnvelope.MAX_FRAMED_BYTES - 8L) {
            throw new IllegalArgumentException("unknown or oversized wrapped WalRun key envelope");
        }
        byte[] canonical = input.readNBytes(Math.toIntExact(declaredLength));
        if (canonical.length != declaredLength) {
            throw new EOFException("truncated wrapped WalRun key envelope");
        }
        ByteArrayOutputStream framed = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(framed)) {
            output.writeShort(kind);
            output.writeShort(version);
            output.writeInt(Math.toIntExact(declaredLength));
            output.write(canonical);
        }
        return WrappedRunKeyEnvelope.decodeFramed(CanonicalBytes.copyOf(framed.toByteArray()));
    }

    private static WalRunPredecessor readPredecessor(DataInputStream input) throws IOException {
        WalRunReference root = readReference(input);
        String sealKey = readUtf8(input, WalRunReference.MAX_METADATA_KEY_BYTES);
        Sha256Digest sealSha256 = readDigest(input);
        Optional<TerminalProtocolCheckpointBindingV1> terminalProtocolCheckpoint = readOptionalFlag(input)
                ? Optional.of(new TerminalProtocolCheckpointBindingV1(
                        com.nereusstream.domain.protocol.ProtocolKindV1.fromCode(input.readUnsignedByte()),
                        readUtf8(input, WalRunReference.MAX_METADATA_KEY_BYTES),
                        readDigest(input)))
                : Optional.empty();
        return new WalRunPredecessor(root, sealKey, sealSha256, terminalProtocolCheckpoint);
    }

    private static void readPreamble(DataInputStream input, int expectedMagic) throws IOException {
        if (input.readInt() != expectedMagic
                || input.readUnsignedByte() != FORMAT_VERSION
                || input.readUnsignedByte() != 0
                || input.readUnsignedShort() != 0) {
            throw new IllegalArgumentException("unknown control record magic/version/reserved bits");
        }
    }

    private static void writeReference(DataOutputStream output, WalRunReference reference) throws IOException {
        writeUtf8(output, reference.rootKey(), WalRunReference.MAX_METADATA_KEY_BYTES);
        writeDigest(output, reference.rootSha256());
        output.writeInt(reference.shardId());
        output.writeLong(reference.shardRunEpoch());
    }

    private static WalRunReference readReference(DataInputStream input) throws IOException {
        return new WalRunReference(
                readUtf8(input, WalRunReference.MAX_METADATA_KEY_BYTES),
                readDigest(input),
                input.readInt(),
                input.readLong());
    }

    private static void writeVector(DataOutputStream output, LaneSequenceVector vector) throws IOException {
        for (long value : vector.toArray()) {
            output.writeLong(value);
        }
    }

    private static LaneSequenceVector readVector(DataInputStream input) throws IOException {
        return LaneSequenceVector.of(input.readLong(), input.readLong(), input.readLong());
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] value = input.readNBytes(Sha256Digest.LENGTH);
        if (value.length != Sha256Digest.LENGTH) {
            throw new EOFException("truncated digest");
        }
        return Sha256Digest.copyOf(value);
    }

    private static void writeUtf8(DataOutputStream output, String value, int maximumBytes) throws IOException {
        writeBytes(output, CanonicalUtf8.fromString(value).bytes(), maximumBytes);
    }

    private static String readUtf8(DataInputStream input, int maximumBytes) throws IOException {
        return CanonicalUtf8.fromBytes(readBytes(input, maximumBytes).toByteArray())
                .value();
    }

    private static void writeBytes(DataOutputStream output, CanonicalBytes bytes, int maximumBytes) throws IOException {
        if (bytes.length() > maximumBytes || bytes.length() > 65_535) {
            throw new IllegalArgumentException("byte field exceeds its canonical bound");
        }
        output.writeShort(bytes.length());
        output.write(bytes.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream input, int maximumBytes) throws IOException {
        int length = input.readUnsignedShort();
        if (length > maximumBytes) {
            throw new IllegalArgumentException("byte field exceeds its canonical bound");
        }
        byte[] value = input.readNBytes(length);
        if (value.length != length) {
            throw new EOFException("truncated byte field");
        }
        return CanonicalBytes.copyOf(value);
    }

    private static void writeOptionalFlag(DataOutputStream output, boolean present) throws IOException {
        output.writeByte(present ? 1 : 0);
    }

    private static boolean readOptionalFlag(DataInputStream input) throws IOException {
        int flag = input.readUnsignedByte();
        if (flag > 1) {
            throw new IllegalArgumentException("optional field flag must be zero or one");
        }
        return flag == 1;
    }

    private static void requireCanonical(CanonicalBytes supplied, CanonicalBytes canonical) {
        if (!Arrays.equals(supplied.toByteArray(), canonical.toByteArray())) {
            throw new IllegalArgumentException("control record is not canonical");
        }
    }

    @FunctionalInterface
    private interface Encoder {
        void encode(DataOutputStream output) throws IOException;
    }

    @FunctionalInterface
    private interface Decoder<T> {
        T decode(DataInputStream input) throws IOException;
    }
}
