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

package com.nereusstream.storage.object.read.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ClosureAnchor;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProofEntry;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProofFold;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.QuiescenceProofHead;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadAdmissionEpochTerminalCut;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadQuiescenceProof;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.TerminalKind;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Strict canonical M4 selector/capability/terminal/proof/head/protection wire. */
public final class M4ReadControlCodecV1 {
    private static final int VERSION = 1;
    private static final int CAPABILITY_MAGIC = 0x4d344331; // M4C1
    private static final int SELECTOR_MAGIC = 0x4d345331; // M4S1
    private static final int ANCHOR_MAGIC = 0x4d344131; // M4A1
    private static final int BATCH_MAGIC = 0x4d344231; // M4B1
    private static final int TERMINAL_MAGIC = 0x4d345431; // M4T1
    private static final int PROOF_MAGIC = 0x4d345031; // M4P1
    private static final int HEAD_MAGIC = 0x4d344831; // M4H1
    private static final int PROTECTION_MAGIC = 0x4d345231; // M4R1
    public static final int MAX_SELECTOR_BYTES = 32 * 1024;
    public static final int EMERGENCY_STOPPED_RESERVE_BYTES = 2 * 1024;
    public static final int MAX_CONTROL_BYTES = 64 * 1024;

    private M4ReadControlCodecV1() {}

    public static CanonicalBytes encodeCapability(CapabilityEvidence value) {
        return encode(out -> {
            preamble(out, CAPABILITY_MAGIC);
            writeCapabilityEvidenceBody(out, value, true);
        });
    }

    public static CapabilityEvidence decodeCapability(CanonicalBytes encoded) {
        CapabilityEvidence result = decode(encoded, input -> {
            requirePreamble(input, CAPABILITY_MAGIC);
            return readCapabilityEvidenceBody(input, true);
        });
        requireCanonical(encoded, encodeCapability(result));
        return result;
    }

    public static Sha256Digest capabilityEvidenceSha256(CapabilityEvidence value) {
        return Sha256Digest.hash(encode(out -> writeCapabilityEvidenceBody(out, value, false)));
    }

    public static CanonicalBytes encodeSelector(BindingReadSelector value) {
        CanonicalBytes encoded = encode(out -> {
            preamble(out, SELECTOR_MAGIC);
            writeSelectorCore(out, value);
            out.writeByte(value.pendingAnchors().size());
            for (ClosureAnchor anchor : value.pendingAnchors()) {
                writeBytes(out, encodeAnchor(anchor), MAX_CONTROL_BYTES);
            }
            out.writeByte(value.activeBatches().size());
            for (SourceRetirementBatch batch : value.activeBatches()) {
                writeBytes(out, encodeBatch(batch), MAX_CONTROL_BYTES);
            }
        });
        if (encoded.length() > MAX_SELECTOR_BYTES) {
            throw new IllegalArgumentException("M4 selector exceeds the 32 KiB hard cap");
        }
        return encoded;
    }

    public static BindingReadSelector decodeSelector(CanonicalBytes encoded) {
        BindingReadSelector result = decode(encoded, input -> {
            requirePreamble(input, SELECTOR_MAGIC);
            SelectorCore core = readSelectorCore(input);
            List<ClosureAnchor> anchors = new ArrayList<>();
            int anchorCount = input.readUnsignedByte();
            if (anchorCount > M4ReadControlRecordsV1.MAX_PENDING_ANCHORS) {
                throw new IllegalArgumentException("M4 selector anchor count exceeds its hard cap");
            }
            for (int index = 0; index < anchorCount; index++) {
                anchors.add(decodeAnchor(readBytes(input, MAX_CONTROL_BYTES)));
            }
            List<SourceRetirementBatch> batches = new ArrayList<>();
            int batchCount = input.readUnsignedByte();
            if (batchCount > M4ReadControlRecordsV1.MAX_ACTIVE_BATCHES) {
                throw new IllegalArgumentException("M4 selector batch count exceeds its hard cap");
            }
            for (int index = 0; index < batchCount; index++) {
                batches.add(decodeBatch(readBytes(input, MAX_CONTROL_BYTES)));
            }
            return core.selector(anchors, batches);
        });
        requireCanonical(encoded, encodeSelector(result));
        return result;
    }

    public static Sha256Digest selectorCoreSha256(BindingReadSelector value) {
        return Sha256Digest.hash(encode(out -> writeSelectorCore(out, value)));
    }

    public static CanonicalBytes encodeAnchor(ClosureAnchor value) {
        return encode(out -> {
            preamble(out, ANCHOR_MAGIC);
            out.writeLong(value.closedReadAdmissionEpoch());
            out.writeLong(value.ownerEpoch());
            writeDigest(out, value.predecessorSelectorCoreSha256());
            writeDigest(out, value.successorSelectorCoreSha256());
            writeDigest(out, value.transitionSha256());
            writeCapabilityBinding(out, value.capability());
        });
    }

    public static ClosureAnchor decodeAnchor(CanonicalBytes encoded) {
        ClosureAnchor result = decode(encoded, input -> {
            requirePreamble(input, ANCHOR_MAGIC);
            return new ClosureAnchor(
                    input.readLong(),
                    input.readLong(),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readCapabilityBinding(input));
        });
        requireCanonical(encoded, encodeAnchor(result));
        return result;
    }

    public static Sha256Digest anchorSha256(ClosureAnchor value) {
        return Sha256Digest.hash(encodeAnchor(value));
    }

    public static CanonicalBytes encodeBatch(SourceRetirementBatch value) {
        validateBatchIdentity(value);
        return encode(out -> {
            preamble(out, BATCH_MAGIC);
            writeBatchBody(out, value, true);
        });
    }

    public static SourceRetirementBatch decodeBatch(CanonicalBytes encoded) {
        SourceRetirementBatch result = decode(encoded, input -> {
            requirePreamble(input, BATCH_MAGIC);
            return readBatchBody(input);
        });
        validateBatchIdentity(result);
        requireCanonical(encoded, encodeBatch(result));
        return result;
    }

    public static Sha256Digest calculateFallbackSetSha256(List<SourceProtectionIdentity> sources) {
        return Sha256Digest.hash(encode(out -> {
            out.writeByte(sources.size());
            for (SourceProtectionIdentity source : sources) {
                writeSourceProtectionIdentity(out, source);
            }
        }));
    }

    public static Sha256Digest calculateBatchId(SourceRetirementBatch value) {
        return Sha256Digest.hash(encode(out -> writeBatchBody(out, value, false)));
    }

    public static CanonicalBytes encodeTerminal(ReadAdmissionEpochTerminalCut value) {
        return encode(out -> {
            preamble(out, TERMINAL_MAGIC);
            writeBinding(out, value.binding());
            writeDigest(out, value.closureAnchorSha256());
            out.writeLong(value.readAdmissionEpoch());
            out.writeLong(value.ownerEpoch());
            out.writeLong(value.lastAdmittedAndDrainedReadViewGeneration());
            out.writeLong(value.safeAfterAuthorityTimeMillis());
            writeCapabilityBinding(out, value.capability());
            out.writeByte(value.kind().ordinal() + 1);
            writeDigest(out, value.admissionClosedOrOwnerFenceSha256());
            writeDigest(out, value.terminalEvidenceSha256());
            out.writeLong(value.authorityNotAfterMillis());
            out.writeLong(value.observedAuthorityTimeMillis());
            out.writeLong(value.reconcilerEpoch());
        });
    }

    public static ReadAdmissionEpochTerminalCut decodeTerminal(CanonicalBytes encoded) {
        ReadAdmissionEpochTerminalCut result = decode(encoded, input -> {
            requirePreamble(input, TERMINAL_MAGIC);
            return new ReadAdmissionEpochTerminalCut(
                    readBinding(input),
                    readDigest(input),
                    input.readLong(),
                    input.readLong(),
                    input.readLong(),
                    input.readLong(),
                    readCapabilityBinding(input),
                    enumValue(TerminalKind.values(), input.readUnsignedByte(), "terminal kind"),
                    readDigest(input),
                    readDigest(input),
                    input.readLong(),
                    input.readLong(),
                    input.readLong());
        });
        requireCanonical(encoded, encodeTerminal(result));
        return result;
    }

    public static Sha256Digest terminalSha256(ReadAdmissionEpochTerminalCut value) {
        return Sha256Digest.hash(encodeTerminal(value));
    }

    public static CanonicalBytes encodeProof(ReadQuiescenceProof value) {
        validateProofIdentity(value);
        return encode(out -> {
            preamble(out, PROOF_MAGIC);
            writeProofBody(out, value, true);
        });
    }

    public static ReadQuiescenceProof decodeProof(CanonicalBytes encoded) {
        ReadQuiescenceProof result = decode(encoded, input -> {
            requirePreamble(input, PROOF_MAGIC);
            return readProofBody(input);
        });
        validateProofIdentity(result);
        requireCanonical(encoded, encodeProof(result));
        return result;
    }

    public static Sha256Digest calculateProofIdentity(ReadQuiescenceProof value) {
        return Sha256Digest.hash(encode(out -> writeProofBody(out, value, false)));
    }

    public static CanonicalBytes encodeHead(QuiescenceProofHead value) {
        return encode(out -> {
            preamble(out, HEAD_MAGIC);
            writeBinding(out, value.binding());
            out.writeLong(value.generation());
            out.writeByte(value.folds().size());
            for (ProofFold fold : value.folds()) {
                out.writeLong(fold.firstEpoch());
                out.writeLong(fold.lastEpoch());
                writeDigest(out, fold.orderedProofsSha256());
            }
            out.writeByte(value.window().size());
            for (ProofEntry entry : value.window()) {
                out.writeLong(entry.readAdmissionEpoch());
                writeDigest(out, entry.proofSha256());
                writeDigest(out, entry.terminalCutSha256());
                writeCapabilityBinding(out, entry.capability());
            }
        });
    }

    public static QuiescenceProofHead decodeHead(CanonicalBytes encoded) {
        QuiescenceProofHead result = decode(encoded, input -> {
            requirePreamble(input, HEAD_MAGIC);
            BindingIdentity binding = readBinding(input);
            long generation = input.readLong();
            int foldCount = input.readUnsignedByte();
            if (foldCount > M4ReadControlRecordsV1.MAX_PROOF_FOLDS) {
                throw new IllegalArgumentException("proof fold count exceeds its hard cap");
            }
            List<ProofFold> folds = new ArrayList<>();
            for (int index = 0; index < foldCount; index++) {
                folds.add(new ProofFold(input.readLong(), input.readLong(), readDigest(input)));
            }
            int windowCount = input.readUnsignedByte();
            if (windowCount > M4ReadControlRecordsV1.MAX_PROOF_WINDOW) {
                throw new IllegalArgumentException("proof window count exceeds its hard cap");
            }
            List<ProofEntry> window = new ArrayList<>();
            for (int index = 0; index < windowCount; index++) {
                window.add(new ProofEntry(
                        input.readLong(), readDigest(input), readDigest(input), readCapabilityBinding(input)));
            }
            return new QuiescenceProofHead(binding, generation, folds, window);
        });
        requireCanonical(encoded, encodeHead(result));
        return result;
    }

    public static CanonicalBytes encodeProtection(SourceProtection value) {
        return encode(out -> {
            preamble(out, PROTECTION_MAGIC);
            writeBinding(out, value.binding());
            writeSourceProtectionIdentity(out, value.identity());
            out.writeByte(value.state().ordinal() + 1);
            writeOptionalDigest(out, value.releasedByBatchSha256());
            writeOptionalDigest(out, value.releaseProofHeadSha256());
        });
    }

    public static SourceProtection decodeProtection(CanonicalBytes encoded) {
        SourceProtection result = decode(encoded, input -> {
            requirePreamble(input, PROTECTION_MAGIC);
            return new SourceProtection(
                    readBinding(input),
                    readSourceProtectionIdentity(input),
                    enumValue(ProtectionState.values(), input.readUnsignedByte(), "protection state"),
                    readOptionalDigest(input),
                    readOptionalDigest(input));
        });
        requireCanonical(encoded, encodeProtection(result));
        return result;
    }

    private static void writeSelectorCore(DataOutputStream out, BindingReadSelector value) throws IOException {
        writeBinding(out, value.binding());
        writeDigest(out, value.selectedViewSha256());
        out.writeLong(value.ownerEpoch());
        out.writeLong(value.readAdmissionEpoch());
        out.writeLong(value.sourceGeneration());
        out.writeByte(value.mode().ordinal() + 1);
        out.writeByte(value.admissionState().ordinal() + 1);
        writeOptionalDigest(out, value.fallbackSetSha256());
        writeCapabilityBinding(out, value.capability());
    }

    private static SelectorCore readSelectorCore(DataInputStream input) throws IOException {
        return new SelectorCore(
                readBinding(input),
                readDigest(input),
                input.readLong(),
                input.readLong(),
                input.readLong(),
                enumValue(SelectorMode.values(), input.readUnsignedByte(), "selector mode"),
                enumValue(AdmissionState.values(), input.readUnsignedByte(), "admission state"),
                readOptionalDigest(input),
                readCapabilityBinding(input));
    }

    private static void writeBatchBody(DataOutputStream out, SourceRetirementBatch value, boolean includeId)
            throws IOException {
        writeBinding(out, value.binding());
        if (includeId) {
            writeDigest(out, value.batchIdSha256());
        }
        writeDigest(out, value.predecessorSelectorCoreSha256());
        writeDigest(out, value.successorSelectorCoreSha256());
        writeDigest(out, value.transitionSha256());
        writeDigest(out, value.fallbackSetSha256());
        out.writeLong(value.sharedLastFallbackCapableReadAdmissionEpoch());
        out.writeLong(value.minimumFirstEpochSummary());
        writeCapabilityBinding(out, value.capability());
        out.writeByte(value.sources().size());
        for (SourceProtectionIdentity source : value.sources()) {
            writeSourceProtectionIdentity(out, source);
        }
    }

    private static SourceRetirementBatch readBatchBody(DataInputStream input) throws IOException {
        BindingIdentity binding = readBinding(input);
        Sha256Digest batchId = readDigest(input);
        Sha256Digest predecessor = readDigest(input);
        Sha256Digest successor = readDigest(input);
        Sha256Digest transition = readDigest(input);
        Sha256Digest fallbackSet = readDigest(input);
        long sharedLast = input.readLong();
        long minimumFirst = input.readLong();
        CapabilityBinding capability = readCapabilityBinding(input);
        int sourceCount = input.readUnsignedByte();
        if (sourceCount == 0 || sourceCount > M4ReadControlRecordsV1.MAX_SOURCES_PER_BATCH) {
            throw new IllegalArgumentException("batch source count exceeds its closed hard cap");
        }
        List<SourceProtectionIdentity> sources = new ArrayList<>();
        for (int index = 0; index < sourceCount; index++) {
            sources.add(readSourceProtectionIdentity(input));
        }
        return new SourceRetirementBatch(
                binding,
                batchId,
                predecessor,
                successor,
                transition,
                fallbackSet,
                sharedLast,
                minimumFirst,
                capability,
                sources);
    }

    private static void validateBatchIdentity(SourceRetirementBatch value) {
        if (!value.fallbackSetSha256().equals(calculateFallbackSetSha256(value.sources()))
                || !value.batchIdSha256().equals(calculateBatchId(value))) {
            throw new IllegalArgumentException("batch fallback-set or deterministic identity digest differs");
        }
    }

    private static void writeProofBody(DataOutputStream out, ReadQuiescenceProof value, boolean includeIdentity)
            throws IOException {
        writeBinding(out, value.binding());
        out.writeLong(value.readAdmissionEpoch());
        writeDigest(out, value.terminalCutSha256());
        out.writeLong(value.drainedThroughReadViewGeneration());
        out.writeLong(value.safeAfterAuthorityTimeMillis());
        writeCapabilityBinding(out, value.capability());
        out.writeByte(value.kind().ordinal() + 1);
        if (includeIdentity) {
            writeDigest(out, value.proofIdentitySha256());
        }
    }

    private static ReadQuiescenceProof readProofBody(DataInputStream input) throws IOException {
        return new ReadQuiescenceProof(
                readBinding(input),
                input.readLong(),
                readDigest(input),
                input.readLong(),
                input.readLong(),
                readCapabilityBinding(input),
                enumValue(TerminalKind.values(), input.readUnsignedByte(), "proof terminal kind"),
                readDigest(input));
    }

    private static void validateProofIdentity(ReadQuiescenceProof value) {
        if (!value.proofIdentitySha256().equals(calculateProofIdentity(value))) {
            throw new IllegalArgumentException("quiescence proof deterministic identity differs");
        }
    }

    private static void writeCapabilityEvidenceBody(
            DataOutputStream out, CapabilityEvidence value, boolean includeState) throws IOException {
        writeBinding(out, value.binding());
        out.writeLong(value.generation());
        out.writeLong(value.backendAdmissionGeneration());
        out.writeByte(value.kind().ordinal() + 1);
        if (includeState) {
            out.writeByte(value.state().ordinal() + 1);
        }
        writeDigest(out, value.backendAdapterSha256());
        writeDigest(out, value.backendProtocolConfigurationSha256());
        writeDigest(out, value.readAdmissionContractSha256());
        writeDigest(out, value.verifierSha256());
        writeDigest(out, value.conformanceReceiptIdentitySha256());
        writeDigest(out, value.conformanceReceiptSha256());
        writeDigest(out, value.authorityTimeSemanticsSha256());
        out.writeLong(value.maximumSourceAccessLifetimeMillis());
        out.writeLong(value.maximumClockSkewMillis());
        out.writeLong(value.propagationGraceMillis());
    }

    private static CapabilityEvidence readCapabilityEvidenceBody(DataInputStream input, boolean includeState)
            throws IOException {
        BindingIdentity binding = readBinding(input);
        long generation = input.readLong();
        long backendAdmissionGeneration = input.readLong();
        CapabilityKind kind = enumValue(CapabilityKind.values(), input.readUnsignedByte(), "capability kind");
        CapabilityState state = includeState
                ? enumValue(CapabilityState.values(), input.readUnsignedByte(), "capability state")
                : CapabilityState.ADMITTED;
        return new CapabilityEvidence(
                binding,
                generation,
                backendAdmissionGeneration,
                kind,
                state,
                readDigest(input),
                readDigest(input),
                readDigest(input),
                readDigest(input),
                readDigest(input),
                readDigest(input),
                readDigest(input),
                input.readLong(),
                input.readLong(),
                input.readLong());
    }

    private static void writeBinding(DataOutputStream out, BindingIdentity value) throws IOException {
        writeDigest(out, value.bindingId().digest());
        writeDigest(out, value.incarnationSha256());
        writeDigest(out, value.storageEpochSha256());
    }

    private static BindingIdentity readBinding(DataInputStream input) throws IOException {
        return new BindingIdentity(new TopicBindingId(readDigest(input)), readDigest(input), readDigest(input));
    }

    private static void writeCapabilityBinding(DataOutputStream out, CapabilityBinding value) throws IOException {
        out.writeLong(value.generation());
        writeDigest(out, value.evidenceSha256());
    }

    private static CapabilityBinding readCapabilityBinding(DataInputStream input) throws IOException {
        return new CapabilityBinding(input.readLong(), readDigest(input));
    }

    private static void writeSourceProtectionIdentity(DataOutputStream out, SourceProtectionIdentity value)
            throws IOException {
        writeDigest(out, value.sourceIdentitySha256());
        out.writeLong(value.protectionGeneration());
        out.writeLong(value.firstFallbackCapableReadAdmissionEpoch());
        out.writeLong(value.fallbackSourceGeneration());
        writeCapabilityBinding(out, value.capability());
    }

    private static SourceProtectionIdentity readSourceProtectionIdentity(DataInputStream input) throws IOException {
        return new SourceProtectionIdentity(
                readDigest(input), input.readLong(), input.readLong(), input.readLong(), readCapabilityBinding(input));
    }

    private static void preamble(DataOutputStream out, int magic) throws IOException {
        out.writeInt(magic);
        out.writeByte(VERSION);
    }

    private static void requirePreamble(DataInputStream input, int magic) throws IOException {
        if (input.readInt() != magic || input.readUnsignedByte() != VERSION) {
            throw new IllegalArgumentException("M4 control record magic/version differs");
        }
    }

    private static void writeDigest(DataOutputStream out, Sha256Digest digest) throws IOException {
        out.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        return Sha256Digest.copyOf(input.readNBytes(Sha256Digest.LENGTH));
    }

    private static void writeOptionalDigest(DataOutputStream out, Optional<Sha256Digest> value) throws IOException {
        out.writeBoolean(value.isPresent());
        if (value.isPresent()) {
            writeDigest(out, value.orElseThrow());
        }
    }

    private static Optional<Sha256Digest> readOptionalDigest(DataInputStream input) throws IOException {
        int flag = input.readUnsignedByte();
        if (flag == 0) {
            return Optional.empty();
        }
        if (flag != 1) {
            throw new IllegalArgumentException("M4 optional digest flag differs");
        }
        return Optional.of(readDigest(input));
    }

    private static void writeBytes(DataOutputStream out, CanonicalBytes value, int maximum) throws IOException {
        if (value.length() > maximum) {
            throw new IllegalArgumentException("nested M4 record exceeds its hard byte cap");
        }
        out.writeInt(value.length());
        out.write(value.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream input, int maximum) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > maximum) {
            throw new IllegalArgumentException("nested M4 record length exceeds its hard cap");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("truncated nested M4 record");
        }
        return CanonicalBytes.copyOf(bytes);
    }

    private static <T extends Enum<T>> T enumValue(T[] values, int code, String label) {
        if (code <= 0 || code > values.length) {
            throw new IllegalArgumentException(label + " discriminator differs: " + code);
        }
        return values[code - 1];
    }

    private static CanonicalBytes encode(Writer writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writer.write(output);
            }
            if (bytes.size() > MAX_CONTROL_BYTES) {
                throw new IllegalArgumentException("M4 canonical control record exceeds 64 KiB");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory M4 control encoding failed", failure);
        }
    }

    private static <T> T decode(CanonicalBytes encoded, Reader<T> reader) {
        if (encoded.length() > MAX_CONTROL_BYTES) {
            throw new IllegalArgumentException("M4 canonical control record exceeds 64 KiB");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            T result = reader.read(input);
            if (input.read() != -1) {
                throw new IllegalArgumentException("M4 canonical control record has trailing bytes");
            }
            return result;
        } catch (IllegalArgumentException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new IllegalArgumentException("cannot decode canonical M4 control record", failure);
        }
    }

    private static void requireCanonical(CanonicalBytes actual, CanonicalBytes canonical) {
        if (!Arrays.equals(actual.toByteArray(), canonical.toByteArray())) {
            throw new IllegalArgumentException("M4 control record is not canonical");
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream output) throws IOException;
    }

    @FunctionalInterface
    private interface Reader<T> {
        T read(DataInputStream input) throws IOException;
    }

    private record SelectorCore(
            BindingIdentity binding,
            Sha256Digest selectedViewSha256,
            long ownerEpoch,
            long readAdmissionEpoch,
            long sourceGeneration,
            SelectorMode mode,
            AdmissionState admissionState,
            Optional<Sha256Digest> fallbackSetSha256,
            CapabilityBinding capability) {
        private BindingReadSelector selector(List<ClosureAnchor> anchors, List<SourceRetirementBatch> batches) {
            return new BindingReadSelector(
                    binding,
                    selectedViewSha256,
                    ownerEpoch,
                    readAdmissionEpoch,
                    sourceGeneration,
                    mode,
                    admissionState,
                    fallbackSetSha256,
                    capability,
                    anchors,
                    batches);
        }
    }
}
