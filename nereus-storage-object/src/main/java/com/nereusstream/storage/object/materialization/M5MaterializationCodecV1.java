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

package com.nereusstream.storage.object.materialization;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.BindingManifestView;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationValidationRoot;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationTask;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializedGeneration;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.OutputPartPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.TaskState;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Strict canonical wire and identity domains for M5-A control records. */
public final class M5MaterializationCodecV1 {
    private static final int VERSION = 1;
    private static final int SOURCE_CUT_MAGIC = 0x4d355343; // M5SC
    private static final int PLAN_MAGIC = 0x4d35504c; // M5PL
    private static final int VALIDATION_MAGIC = 0x4d355652; // M5VR
    private static final int GENERATION_MAGIC = 0x4d354745; // M5GE
    private static final int MANIFEST_MAGIC = 0x4d354d56; // M5MV
    private static final int TASK_MAGIC = 0x4d35544b; // M5TK
    private static final int MAX_CONTROL_BYTES = 2 * 1024 * 1024;
    private static final int MAX_TEXT_BYTES = 4 * 1024;
    private static final int MAX_EMBEDDED_SELECTOR_BYTES = M4ReadControlCodecV1.MAX_SELECTOR_BYTES;

    private M5MaterializationCodecV1() {}

    public static Sha256Digest calculateSourceSetSha256(List<SourceExtent> sources) {
        return domainHash("NEREUS_V2_M5_SOURCE_SET_V1", out -> {
            out.writeInt(sources.size());
            for (SourceExtent source : sources) {
                writeSource(out, source);
            }
        });
    }

    public static CanonicalBytes encodeSourceCut(MaterializationSourceCut value) {
        verifySourceSet(value);
        return encode(out -> {
            preamble(out, SOURCE_CUT_MAGIC);
            writeEnvelope(out, value.identity());
            writeBytes(
                    out, M4ReadControlCodecV1.encodeSelector(value.predecessorSelector()), MAX_EMBEDDED_SELECTOR_BYTES);
            writeDigest(out, value.predecessorSelectorValueSha256());
            writeDigest(out, value.predecessorViewSha256());
            writeCoverage(out, value.coverage());
            out.writeLong(value.durableFrontier());
            out.writeLong(value.logEndFrontier());
            out.writeLong(value.highWatermark());
            out.writeLong(value.lastStableFrontier());
            out.writeLong(value.trimFrontier());
            writeDigest(out, value.protocolStateRootSha256());
            writeDigest(out, value.recoveryCheckpointRootSha256());
            writeDigest(out, value.materializationPolicySha256());
            writeDigest(out, value.outputFormatPolicySha256());
            writeDigest(out, value.sourceSetSha256());
            out.writeInt(value.sources().size());
            for (SourceExtent source : value.sources()) {
                writeSource(out, source);
            }
        });
    }

    public static MaterializationSourceCut decodeSourceCut(CanonicalBytes encoded) {
        MaterializationSourceCut result = decode(encoded, input -> {
            requirePreamble(input, SOURCE_CUT_MAGIC);
            IdentityEnvelope identity = readEnvelope(input);
            BindingReadSelector predecessor =
                    M4ReadControlCodecV1.decodeSelector(readBytes(input, MAX_EMBEDDED_SELECTOR_BYTES));
            Sha256Digest predecessorValue = readDigest(input);
            Sha256Digest predecessorView = readDigest(input);
            ProtocolCoverage coverage = readCoverage(input);
            long durable = input.readLong();
            long logEnd = input.readLong();
            long highWatermark = input.readLong();
            long lastStable = input.readLong();
            long trim = input.readLong();
            Sha256Digest protocolState = readDigest(input);
            Sha256Digest recovery = readDigest(input);
            Sha256Digest materializationPolicy = readDigest(input);
            Sha256Digest outputPolicy = readDigest(input);
            Sha256Digest sourceSet = readDigest(input);
            int count = boundedCount(input, M5MaterializationRecordsV1.MAX_SOURCES, "source count", false);
            List<SourceExtent> sources = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                sources.add(readSource(input));
            }
            return new MaterializationSourceCut(
                    identity,
                    predecessor,
                    predecessorValue,
                    predecessorView,
                    coverage,
                    durable,
                    logEnd,
                    highWatermark,
                    lastStable,
                    trim,
                    protocolState,
                    recovery,
                    materializationPolicy,
                    outputPolicy,
                    sourceSet,
                    sources);
        });
        requireCanonical(encoded, encodeSourceCut(result));
        return result;
    }

    public static Sha256Digest sourceCutSha256(MaterializationSourceCut value) {
        return Sha256Digest.hash(encodeSourceCut(value));
    }

    public static Sha256Digest calculateTaskId(MaterializationSourceCut cut) {
        verifySourceSet(cut);
        return domainHash("NEREUS_V2_M5_MATERIALIZATION_TASK_V1", out -> {
            writeEnvelope(out, cut.identity());
            writeCoverage(out, cut.coverage());
            writeDigest(out, cut.predecessorSelectorValueSha256());
            writeDigest(out, cut.sourceSetSha256());
            writeDigest(out, cut.protocolStateRootSha256());
            writeDigest(out, cut.materializationPolicySha256());
            writeDigest(out, cut.outputFormatPolicySha256());
        });
    }

    public static Sha256Digest calculateOutputIdentity(
            MaterializationSourceCut cut,
            RepresentationMode mode,
            PayloadKind payloadKind,
            Sha256Digest taskId,
            Sha256Digest encryptionGeneration,
            Sha256Digest compressionPolicy,
            Sha256Digest checksumPolicy,
            List<OutputPartPlan> parts,
            List<IndexPlan> indexes) {
        return domainHash("NEREUS_V2_M5_MATERIALIZATION_OUTPUT_V1", out -> {
            writeDigest(out, sourceCutSha256(cut));
            out.writeByte(mode.ordinal());
            out.writeByte(payloadKind.ordinal());
            writeDigest(out, taskId);
            writeDigest(out, encryptionGeneration);
            writeDigest(out, compressionPolicy);
            writeDigest(out, checksumPolicy);
            writeParts(out, parts);
            writeIndexes(out, indexes);
        });
    }

    public static CanonicalBytes encodePlan(MaterializationPlan value) {
        verifyPlanIdentities(value);
        return encode(out -> {
            preamble(out, PLAN_MAGIC);
            writeBytes(out, encodeSourceCut(value.sourceCut()), MAX_CONTROL_BYTES);
            out.writeByte(value.representationMode().ordinal());
            out.writeByte(value.payloadKind().ordinal());
            writeDigest(out, value.taskIdSha256());
            writeDigest(out, value.outputIdentitySha256());
            writeDigest(out, value.encryptionGenerationSha256());
            writeDigest(out, value.compressionPolicySha256());
            writeDigest(out, value.checksumPolicySha256());
            writeParts(out, value.outputParts());
            writeIndexes(out, value.indexes());
        });
    }

    public static MaterializationPlan decodePlan(CanonicalBytes encoded) {
        MaterializationPlan result = decode(encoded, input -> {
            requirePreamble(input, PLAN_MAGIC);
            MaterializationSourceCut cut = decodeSourceCut(readBytes(input, MAX_CONTROL_BYTES));
            RepresentationMode mode = enumValue(RepresentationMode.values(), input.readUnsignedByte(), "mode");
            PayloadKind payloadKind = enumValue(PayloadKind.values(), input.readUnsignedByte(), "payload kind");
            Sha256Digest taskId = readDigest(input);
            Sha256Digest outputId = readDigest(input);
            Sha256Digest encryption = readDigest(input);
            Sha256Digest compression = readDigest(input);
            Sha256Digest checksum = readDigest(input);
            List<OutputPartPlan> parts = readParts(input);
            List<IndexPlan> indexes = readIndexes(input);
            return new MaterializationPlan(
                    cut, mode, payloadKind, taskId, outputId, encryption, compression, checksum, parts, indexes);
        });
        requireCanonical(encoded, encodePlan(result));
        return result;
    }

    public static CanonicalBytes encodeValidationRoot(GenerationValidationRoot value) {
        return encode(out -> {
            preamble(out, VALIDATION_MAGIC);
            writeDigest(out, value.taskIdSha256());
            writeDigest(out, value.outputIdentitySha256());
            writeDigest(out, value.sourceSetSha256());
            writeDigest(out, value.validatedObjectsRootSha256());
            writeDigest(out, value.coverageRootSha256());
            writeDigest(out, value.lookupBoundaryRootSha256());
            writeDigest(out, value.payloadEqualityOrSemanticRootSha256());
            writeDigest(out, value.authorityFenceRootSha256());
            out.writeInt(value.validatedPayloadObjects());
            out.writeInt(value.validatedIndexObjects());
            out.writeLong(value.validatedCanonicalBytes());
        });
    }

    public static GenerationValidationRoot decodeValidationRoot(CanonicalBytes encoded) {
        GenerationValidationRoot result = decode(encoded, input -> {
            requirePreamble(input, VALIDATION_MAGIC);
            return new GenerationValidationRoot(
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    input.readInt(),
                    input.readInt(),
                    input.readLong());
        });
        requireCanonical(encoded, encodeValidationRoot(result));
        return result;
    }

    public static CanonicalBytes encodeGeneration(MaterializedGeneration value) {
        return encode(out -> {
            preamble(out, GENERATION_MAGIC);
            writeEnvelope(out, value.identity());
            out.writeByte(value.representationMode().ordinal());
            out.writeByte(value.payloadKind().ordinal());
            writeDigest(out, value.taskIdSha256());
            writeDigest(out, value.outputIdentitySha256());
            writeDigest(out, value.sourceSetSha256());
            out.writeLong(value.sourceGeneration());
            writeCoverage(out, value.coverage());
            writeDigest(out, value.protocolStateRootSha256());
            writeOptionalDigest(out, value.semanticProofRootSha256());
            writeDigest(out, value.validationRootSha256());
            writeDigest(out, value.predecessorSelectedViewSha256());
            writeOptionalDigest(out, value.fallbackSetSha256());
            writeGenerationObjects(out, value.payloadObjects());
            writeGenerationObjects(out, value.indexObjects());
        });
    }

    public static MaterializedGeneration decodeGeneration(CanonicalBytes encoded) {
        MaterializedGeneration result = decode(encoded, input -> {
            requirePreamble(input, GENERATION_MAGIC);
            return new MaterializedGeneration(
                    readEnvelope(input),
                    enumValue(RepresentationMode.values(), input.readUnsignedByte(), "mode"),
                    enumValue(PayloadKind.values(), input.readUnsignedByte(), "payload kind"),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    input.readLong(),
                    readCoverage(input),
                    readDigest(input),
                    readOptionalDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readOptionalDigest(input),
                    readGenerationObjects(input),
                    readGenerationObjects(input));
        });
        requireCanonical(encoded, encodeGeneration(result));
        return result;
    }

    public static CanonicalBytes encodeManifest(BindingManifestView value) {
        return encode(out -> {
            preamble(out, MANIFEST_MAGIC);
            writeEnvelope(out, value.identity());
            writeDigest(out, value.preferredGenerationSha256());
            writeDigest(out, value.exactPredecessorViewSha256());
            writeDigest(out, value.exactPredecessorSelectorValueSha256());
            writeCoverage(out, value.coverage());
            writeOptionalDigest(out, value.compactionSuppressionRootSha256());
        });
    }

    public static BindingManifestView decodeManifest(CanonicalBytes encoded) {
        BindingManifestView result = decode(encoded, input -> {
            requirePreamble(input, MANIFEST_MAGIC);
            return new BindingManifestView(
                    readEnvelope(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readCoverage(input),
                    readOptionalDigest(input));
        });
        requireCanonical(encoded, encodeManifest(result));
        return result;
    }

    public static CanonicalBytes encodeTask(MaterializationTask value) {
        return encode(out -> {
            preamble(out, TASK_MAGIC);
            writeDigest(out, value.taskIdSha256());
            out.writeByte(value.state().ordinal());
            writeDigest(out, value.sourceCutSha256());
            writeDigest(out, value.outputIdentitySha256());
            writeOptionalDigest(out, value.validationRootSha256());
            writeOptionalDigest(out, value.generationSha256());
            writeOptionalDigest(out, value.manifestViewSha256());
        });
    }

    public static MaterializationTask decodeTask(CanonicalBytes encoded) {
        MaterializationTask result = decode(encoded, input -> {
            requirePreamble(input, TASK_MAGIC);
            return new MaterializationTask(
                    readDigest(input),
                    enumValue(TaskState.values(), input.readUnsignedByte(), "task state"),
                    readDigest(input),
                    readDigest(input),
                    readOptionalDigest(input),
                    readOptionalDigest(input),
                    readOptionalDigest(input));
        });
        requireCanonical(encoded, encodeTask(result));
        return result;
    }

    public static Sha256Digest generationSha256(MaterializedGeneration value) {
        return Sha256Digest.hash(encodeGeneration(value));
    }

    public static Sha256Digest validationRootSha256(GenerationValidationRoot value) {
        return Sha256Digest.hash(encodeValidationRoot(value));
    }

    public static Sha256Digest manifestSha256(BindingManifestView value) {
        return Sha256Digest.hash(encodeManifest(value));
    }

    private static void verifySourceSet(MaterializationSourceCut value) {
        if (!calculateSourceSetSha256(value.sources()).equals(value.sourceSetSha256())) {
            throw new IllegalArgumentException("source-set digest differs from canonical membership");
        }
        Sha256Digest selectorSha = Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(value.predecessorSelector()));
        if (!selectorSha.equals(value.predecessorSelectorValueSha256())) {
            throw new IllegalArgumentException("predecessor selector value digest differs");
        }
    }

    private static void verifyPlanIdentities(MaterializationPlan value) {
        Sha256Digest taskId = calculateTaskId(value.sourceCut());
        if (!taskId.equals(value.taskIdSha256())) {
            throw new IllegalArgumentException("materialization task identity differs");
        }
        Sha256Digest outputId = calculateOutputIdentity(
                value.sourceCut(),
                value.representationMode(),
                value.payloadKind(),
                taskId,
                value.encryptionGenerationSha256(),
                value.compressionPolicySha256(),
                value.checksumPolicySha256(),
                value.outputParts(),
                value.indexes());
        if (!outputId.equals(value.outputIdentitySha256())) {
            throw new IllegalArgumentException("materialization output identity differs");
        }
    }

    private static void writeEnvelope(DataOutputStream out, IdentityEnvelope value) throws IOException {
        writeDigest(out, value.protocolCellSha256());
        writeDigest(out, value.providerScopeSha256());
        writeDigest(out, value.binding().bindingId().digest());
        writeDigest(out, value.binding().incarnationSha256());
        writeDigest(out, value.binding().storageEpochSha256());
        out.writeLong(value.ownerEpoch());
        out.writeLong(value.workerEpoch());
        out.writeLong(value.storageFence());
        out.writeLong(value.capability().generation());
        writeDigest(out, value.capability().evidenceSha256());
    }

    private static IdentityEnvelope readEnvelope(DataInputStream input) throws IOException {
        Sha256Digest cell = readDigest(input);
        Sha256Digest scope = readDigest(input);
        BindingIdentity binding =
                new BindingIdentity(new TopicBindingId(readDigest(input)), readDigest(input), readDigest(input));
        long owner = input.readLong();
        long worker = input.readLong();
        long storage = input.readLong();
        CapabilityBinding capability = new CapabilityBinding(input.readLong(), readDigest(input));
        return new IdentityEnvelope(cell, scope, binding, owner, worker, storage, capability);
    }

    private static void writeSource(DataOutputStream out, SourceExtent value) throws IOException {
        out.writeByte(value.kind().ordinal());
        writeDigest(out, value.sourceIdentitySha256());
        writeCoverage(out, value.coverage());
        writeText(out, value.physicalKey());
        out.writeLong(value.canonicalLength());
        out.writeInt(value.recordCount());
        out.writeLong(value.minimumTimestamp());
        out.writeLong(value.maximumTimestamp());
        writeDigest(out, value.bodySha256());
        writeOptionalBytes(out, value.immutableProviderVersionToken());
        writeOptionalDigest(out, value.ledgerIdentitySha256());
        writeDigest(out, value.formatRootSha256());
        writeDigest(out, value.encryptionPolicySha256());
        out.writeBoolean(value.payloadLongLivedReadable());
        out.writeBoolean(value.requiredIndexesPresent());
        out.writeInt(value.memberBindingIds().size());
        for (Sha256Digest member : value.memberBindingIds()) {
            writeDigest(out, member);
        }
    }

    private static SourceExtent readSource(DataInputStream input) throws IOException {
        SourceKind kind = enumValue(SourceKind.values(), input.readUnsignedByte(), "source kind");
        Sha256Digest identity = readDigest(input);
        ProtocolCoverage coverage = readCoverage(input);
        String key = readText(input);
        long length = input.readLong();
        int recordCount = input.readInt();
        long minimumTimestamp = input.readLong();
        long maximumTimestamp = input.readLong();
        Sha256Digest body = readDigest(input);
        Optional<CanonicalBytes> token = readOptionalBytes(input);
        Optional<Sha256Digest> ledger = readOptionalDigest(input);
        Sha256Digest format = readDigest(input);
        Sha256Digest encryption = readDigest(input);
        boolean readable = input.readBoolean();
        boolean indexed = input.readBoolean();
        int members = boundedCount(input, M5MaterializationRecordsV1.MAX_MEMBER_BINDINGS, "member bindings", false);
        List<Sha256Digest> memberBindings = new ArrayList<>(members);
        for (int index = 0; index < members; index++) {
            memberBindings.add(readDigest(input));
        }
        return new SourceExtent(
                kind,
                identity,
                coverage,
                key,
                length,
                recordCount,
                minimumTimestamp,
                maximumTimestamp,
                body,
                token,
                ledger,
                format,
                encryption,
                readable,
                indexed,
                memberBindings);
    }

    private static void writeParts(DataOutputStream out, List<OutputPartPlan> values) throws IOException {
        out.writeInt(values.size());
        for (OutputPartPlan value : values) {
            out.writeInt(value.ordinal());
            writeCoverage(out, value.coverage());
            out.writeByte(value.payloadKind().ordinal());
            writeDigest(out, value.canonicalPlanSha256());
            writeText(out, value.objectKey());
        }
    }

    private static List<OutputPartPlan> readParts(DataInputStream input) throws IOException {
        int count = boundedCount(input, M5MaterializationRecordsV1.MAX_PARTS, "part count", false);
        List<OutputPartPlan> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(new OutputPartPlan(
                    input.readInt(),
                    readCoverage(input),
                    enumValue(PayloadKind.values(), input.readUnsignedByte(), "payload kind"),
                    readDigest(input),
                    readText(input)));
        }
        return result;
    }

    private static void writeIndexes(DataOutputStream out, List<IndexPlan> values) throws IOException {
        out.writeInt(values.size());
        for (IndexPlan value : values) {
            out.writeByte(value.kind().ordinal());
            writeCoverage(out, value.coverage());
            out.writeInt(value.parserVersion());
            writeDigest(out, value.canonicalPlanSha256());
            writeText(out, value.objectKey());
        }
    }

    private static List<IndexPlan> readIndexes(DataInputStream input) throws IOException {
        int count = boundedCount(input, M5MaterializationRecordsV1.MAX_INDEXES, "index count", true);
        List<IndexPlan> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            result.add(new IndexPlan(
                    enumValue(IndexKind.values(), input.readUnsignedByte(), "index kind"),
                    readCoverage(input),
                    input.readInt(),
                    readDigest(input),
                    readText(input)));
        }
        return result;
    }

    private static void writeGenerationObjects(DataOutputStream out, List<GenerationObject> values) throws IOException {
        out.writeInt(values.size());
        for (GenerationObject value : values) {
            out.writeInt(value.ordinal());
            out.writeInt(value.indexKind() == null ? -1 : value.indexKind().ordinal());
            writeCoverage(out, value.coverage());
            writeText(out, value.identity().key());
            out.writeLong(value.identity().bodyLength());
            writeDigest(out, value.identity().bodySha256());
            writeOptionalBytes(out, value.immutableProviderVersionToken());
        }
    }

    private static List<GenerationObject> readGenerationObjects(DataInputStream input) throws IOException {
        int count = boundedCount(input, M5MaterializationRecordsV1.MAX_PARTS, "generation Object count", true);
        List<GenerationObject> result = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            int ordinal = input.readInt();
            int rawKind = input.readInt();
            IndexKind kind = rawKind == -1 ? null : enumValue(IndexKind.values(), rawKind, "index kind");
            ProtocolCoverage coverage = readCoverage(input);
            ObjectIdentity identity = new ObjectIdentity(readText(input), input.readLong(), readDigest(input));
            result.add(new GenerationObject(ordinal, kind, coverage, identity, readOptionalBytes(input)));
        }
        return result;
    }

    private static void writeCoverage(DataOutputStream out, ProtocolCoverage value) throws IOException {
        out.writeByte(value.domain().ordinal());
        out.writeLong(value.inclusiveStart());
        out.writeLong(value.exclusiveEnd());
    }

    private static ProtocolCoverage readCoverage(DataInputStream input) throws IOException {
        return new ProtocolCoverage(
                enumValue(PositionDomain.values(), input.readUnsignedByte(), "position domain"),
                input.readLong(),
                input.readLong());
    }

    private static Sha256Digest domainHash(String domain, Writer writer) {
        return Sha256Digest.hash(encode(out -> {
            writeText(out, domain);
            writer.write(out);
        }));
    }

    private static void preamble(DataOutputStream out, int magic) throws IOException {
        out.writeInt(magic);
        out.writeInt(VERSION);
    }

    private static void requirePreamble(DataInputStream input, int magic) throws IOException {
        if (input.readInt() != magic || input.readInt() != VERSION) {
            throw new IllegalArgumentException("M5 materialization magic/version differs");
        }
    }

    private static void writeDigest(DataOutputStream out, Sha256Digest value) throws IOException {
        out.write(value.bytes().toByteArray());
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
        return input.readBoolean() ? Optional.of(readDigest(input)) : Optional.empty();
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0
                || bytes.length > MAX_TEXT_BYTES
                || !new String(bytes, StandardCharsets.UTF_8).equals(value)) {
            throw new IllegalArgumentException("M5 text is empty, oversized, or non-canonical");
        }
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readUnsignedShort();
        if (length == 0 || length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException("M5 text length is outside its cap");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("truncated M5 text");
        }
        String value = new String(bytes, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(bytes, value.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("M5 text is not canonical UTF-8");
        }
        return value;
    }

    private static void writeOptionalBytes(DataOutputStream out, Optional<CanonicalBytes> value) throws IOException {
        out.writeBoolean(value.isPresent());
        if (value.isPresent()) {
            writeBytes(out, value.orElseThrow(), 65_535);
        }
    }

    private static Optional<CanonicalBytes> readOptionalBytes(DataInputStream input) throws IOException {
        return input.readBoolean() ? Optional.of(readBytes(input, 65_535)) : Optional.empty();
    }

    private static void writeBytes(DataOutputStream out, CanonicalBytes value, int maximum) throws IOException {
        if (value.isEmpty() || value.length() > maximum) {
            throw new IllegalArgumentException("embedded M5 value length is outside its cap");
        }
        out.writeInt(value.length());
        out.write(value.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream input, int maximum) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > maximum) {
            throw new IllegalArgumentException("embedded M5 value length is outside its cap");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("truncated embedded M5 value");
        }
        return CanonicalBytes.copyOf(bytes);
    }

    private static int boundedCount(DataInputStream input, int maximum, String label, boolean allowZero)
            throws IOException {
        int count = input.readInt();
        if (count < (allowZero ? 0 : 1) || count > maximum) {
            throw new IllegalArgumentException(label + " is outside its cap");
        }
        return count;
    }

    private static <T extends Enum<T>> T enumValue(T[] values, int code, String label) {
        if (code < 0 || code >= values.length) {
            throw new IllegalArgumentException(label + " code is unknown: " + code);
        }
        return values[code];
    }

    private static CanonicalBytes encode(Writer writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writer.write(output);
            }
            if (bytes.size() <= 0 || bytes.size() > MAX_CONTROL_BYTES) {
                throw new IllegalArgumentException("M5 control value exceeds its canonical byte cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException error) {
            throw new IllegalStateException("in-memory M5 encoding failed", error);
        }
    }

    private static <T> T decode(CanonicalBytes encoded, Reader<T> reader) {
        if (encoded == null || encoded.isEmpty() || encoded.length() > MAX_CONTROL_BYTES) {
            throw new IllegalArgumentException("M5 control value length is outside its cap");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            T value = reader.read(input);
            if (input.read() != -1) {
                throw new IllegalArgumentException("M5 control value has trailing bytes");
            }
            return value;
        } catch (IOException error) {
            throw new IllegalArgumentException("invalid M5 control value", error);
        }
    }

    private static void requireCanonical(CanonicalBytes actual, CanonicalBytes expected) {
        if (!actual.equals(expected)) {
            throw new IllegalArgumentException("M5 control value is not canonical");
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
}
