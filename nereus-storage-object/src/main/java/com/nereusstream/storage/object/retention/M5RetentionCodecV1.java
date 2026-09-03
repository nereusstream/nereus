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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BindingTrimFrontierV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FullSourceRetirementBatchV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.M4ReleaseBindingV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceScanSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredSourceRetirementBatchTombstoneV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredTopicIncarnationTombstoneV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Strict canonical wire for every M5-C persistent value. */
public final class M5RetentionCodecV1 {
    private static final int VERSION = 1;
    private static final int SNAPSHOT_MAGIC = 0x4d355346; // M5SF
    private static final int TRIM_MAGIC = 0x4d355446; // M5TF
    private static final int PROOF_MAGIC = 0x4d355246; // M5RF
    private static final int FULL_BATCH_MAGIC = 0x4d354246; // M5BF
    private static final int RETIRED_BATCH_MAGIC = 0x4d354252; // M5BR
    private static final int RETIRED_PULSAR_MAGIC = 0x4d355052; // M5PR
    public static final int MAX_CONTROL_BYTES = 1024 * 1024;
    private static final int MAX_TEXT_BYTES = 16 * 1024;
    private static final int MAX_VERSION_BYTES = 256;
    private static final Sha256Digest PLACEHOLDER = Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1}));

    private M5RetentionCodecV1() {}

    public static RetentionFloorSnapshotV1 finalizeSnapshot(RetentionFloorSnapshotV1 draft) {
        RetentionFloorSnapshotV1 placeholder = new RetentionFloorSnapshotV1(
                draft.identity(),
                draft.domain(),
                draft.generation(),
                draft.priorTrimFrontier(),
                draft.retentionPolicyRootSha256(),
                draft.ownerFence(),
                draft.storageFence(),
                draft.pageCount(),
                draft.scannedBytes(),
                draft.rows(),
                PLACEHOLDER);
        return new RetentionFloorSnapshotV1(
                draft.identity(),
                draft.domain(),
                draft.generation(),
                draft.priorTrimFrontier(),
                draft.retentionPolicyRootSha256(),
                draft.ownerFence(),
                draft.storageFence(),
                draft.pageCount(),
                draft.scannedBytes(),
                draft.rows(),
                calculateSnapshotRoot(placeholder));
    }

    public static CanonicalBytes encodeSnapshot(RetentionFloorSnapshotV1 value) {
        if (!value.snapshotRootSha256().equals(calculateSnapshotRoot(value))) {
            throw new IllegalArgumentException("retention snapshot identity differs from its canonical body");
        }
        return encode(out -> {
            preamble(out, SNAPSHOT_MAGIC);
            writeSnapshotBody(out, value);
            writeDigest(out, value.snapshotRootSha256());
        });
    }

    public static RetentionFloorSnapshotV1 decodeSnapshot(CanonicalBytes encoded) {
        RetentionFloorSnapshotV1 value = decode(encoded, input -> {
            requirePreamble(input, SNAPSHOT_MAGIC);
            IdentityEnvelope identity = readEnvelope(input);
            PositionDomain domain = enumValue(PositionDomain.values(), input.readUnsignedByte(), "Position Domain");
            long generation = input.readLong();
            long prior = input.readLong();
            Sha256Digest policy = readDigest(input);
            AuthorityFactV1 owner = readAuthorityFact(input);
            AuthorityFactV1 storage = readAuthorityFact(input);
            int pages = input.readInt();
            long bytes = input.readLong();
            int count = boundedCount(input.readInt(), M5RetentionRecordsV1.MAX_FLOOR_ROWS, "floor rows");
            List<RetentionFloorObservationV1> rows = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                rows.add(readFloor(input));
            }
            return new RetentionFloorSnapshotV1(
                    identity, domain, generation, prior, policy, owner, storage, pages, bytes, rows, readDigest(input));
        });
        requireCanonical(encoded, encodeSnapshot(value));
        return value;
    }

    public static Sha256Digest calculateSnapshotRoot(RetentionFloorSnapshotV1 value) {
        return Sha256Digest.hash(encode(out -> writeSnapshotBody(out, value)));
    }

    public static CanonicalBytes encodeTrimFrontier(BindingTrimFrontierV1 value) {
        return encode(out -> {
            preamble(out, TRIM_MAGIC);
            writeEnvelope(out, value.identity());
            out.writeByte(value.domain().ordinal() + 1);
            out.writeLong(value.priorFrontier());
            out.writeLong(value.newFrontier());
            writeDigest(out, value.retentionPolicyRootSha256());
            writeDigest(out, value.floorSnapshotRootSha256());
            writeAuthorityFact(out, value.ownerFence());
            writeAuthorityFact(out, value.storageFence());
            out.writeLong(value.generation());
            writeCapability(out, value.capability());
        });
    }

    public static BindingTrimFrontierV1 decodeTrimFrontier(CanonicalBytes encoded) {
        BindingTrimFrontierV1 value = decode(encoded, input -> {
            requirePreamble(input, TRIM_MAGIC);
            return new BindingTrimFrontierV1(
                    readEnvelope(input),
                    enumValue(PositionDomain.values(), input.readUnsignedByte(), "Position Domain"),
                    input.readLong(),
                    input.readLong(),
                    readDigest(input),
                    readDigest(input),
                    readAuthorityFact(input),
                    readAuthorityFact(input),
                    input.readLong(),
                    readCapability(input));
        });
        requireCanonical(encoded, encodeTrimFrontier(value));
        return value;
    }

    public static ReferenceFreeProofV1 finalizeProof(ReferenceFreeProofV1 draft) {
        ReferenceFreeProofV1 placeholder = copyProof(draft, PLACEHOLDER);
        return copyProof(draft, calculateProofSha256(placeholder));
    }

    public static CanonicalBytes encodeReferenceFreeProof(ReferenceFreeProofV1 value) {
        if (!value.observationsRootSha256().equals(calculateObservationsRoot(value.observations()))) {
            throw new IllegalArgumentException("reference observation root differs from its canonical rows");
        }
        if (!value.proofSha256().equals(calculateProofSha256(value))) {
            throw new IllegalArgumentException("reference-free proof identity differs from its canonical body");
        }
        return encode(out -> {
            preamble(out, PROOF_MAGIC);
            writeProofBody(out, value);
            writeDigest(out, value.proofSha256());
        });
    }

    public static ReferenceFreeProofV1 decodeReferenceFreeProof(CanonicalBytes encoded) {
        ReferenceFreeProofV1 value = decode(encoded, input -> {
            requirePreamble(input, PROOF_MAGIC);
            IdentityEnvelope identity = readEnvelope(input);
            ReferenceTargetKindV1 targetKind =
                    enumValue(ReferenceTargetKindV1.values(), input.readUnsignedByte(), "target kind");
            Sha256Digest target = readDigest(input);
            ProtocolCoverage coverage = readCoverage(input);
            AuthorityFactV1 selector = readAuthorityFact(input);
            AuthorityFactV1 manifest = readAuthorityFact(input);
            AuthorityFactV1 trim = readAuthorityFact(input);
            Sha256Digest snapshot = readDigest(input);
            Sha256Digest observationsRoot = readDigest(input);
            int releaseCount =
                    boundedOptionalCount(input.readInt(), M5RetentionRecordsV1.MAX_RELEASE_BINDINGS, "M4 releases");
            List<M4ReleaseBindingV1> releases = new ArrayList<>(releaseCount);
            for (int index = 0; index < releaseCount; index++) {
                releases.add(readRelease(input));
            }
            AuthorityFactV1 owner = readAuthorityFact(input);
            AuthorityFactV1 worker = readAuthorityFact(input);
            AuthorityFactV1 storage = readAuthorityFact(input);
            AuthorityFactV1 provider = readAuthorityFact(input);
            long audit = input.readLong();
            long observed = input.readLong();
            int summaryCount = boundedCount(input.readInt(), ReferenceKindV1.values().length, "scan summaries");
            List<ReferenceScanSummaryV1> summaries = new ArrayList<>(summaryCount);
            for (int index = 0; index < summaryCount; index++) {
                summaries.add(readSummary(input));
            }
            int observationCount =
                    boundedCount(input.readInt(), M5RetentionRecordsV1.MAX_REFERENCE_ROWS, "reference observations");
            List<ReferenceObservationV1> observations = new ArrayList<>(observationCount);
            for (int index = 0; index < observationCount; index++) {
                observations.add(readObservation(input));
            }
            return new ReferenceFreeProofV1(
                    identity,
                    targetKind,
                    target,
                    coverage,
                    selector,
                    manifest,
                    trim,
                    snapshot,
                    observationsRoot,
                    releases,
                    owner,
                    worker,
                    storage,
                    provider,
                    audit,
                    observed,
                    summaries,
                    observations,
                    readDigest(input));
        });
        requireCanonical(encoded, encodeReferenceFreeProof(value));
        return value;
    }

    public static Sha256Digest calculateProofSha256(ReferenceFreeProofV1 value) {
        return Sha256Digest.hash(encode(out -> writeProofBody(out, value)));
    }

    public static Sha256Digest calculateObservationsRoot(List<ReferenceObservationV1> rows) {
        return Sha256Digest.hash(encode(out -> {
            out.writeInt(rows.size());
            for (ReferenceObservationV1 row : rows) {
                writeObservation(out, row);
            }
        }));
    }

    public static CanonicalBytes encodeFullBatch(FullSourceRetirementBatchV1 value) {
        return encode(out -> {
            preamble(out, FULL_BATCH_MAGIC);
            out.writeByte(value.state().ordinal() + 1);
            writeBinding(out, value.binding());
            writeDigest(out, value.batchIdSha256());
            writeDigest(out, value.fullBatchSha256());
            writeBytes(out, value.canonicalM4BatchBytes(), M5RetentionRecordsV1.MAX_FULL_BATCH_BYTES);
            writeDigest(out, value.selectorPredecessorValueSha256());
            writeDigest(out, value.referenceFreeProofSha256());
            writeCapability(out, value.capability());
        });
    }

    public static FullSourceRetirementBatchV1 decodeFullBatch(CanonicalBytes encoded) {
        FullSourceRetirementBatchV1 value = decode(encoded, input -> {
            requirePreamble(input, FULL_BATCH_MAGIC);
            return new FullSourceRetirementBatchV1(
                    enumValue(BatchMetadataStateV1.values(), input.readUnsignedByte(), "batch state"),
                    readBinding(input),
                    readDigest(input),
                    readDigest(input),
                    readBytes(input, M5RetentionRecordsV1.MAX_FULL_BATCH_BYTES),
                    readDigest(input),
                    readDigest(input),
                    readCapability(input));
        });
        requireCanonical(encoded, encodeFullBatch(value));
        return value;
    }

    public static RetiredSourceRetirementBatchTombstoneV1 finalizeRetiredBatch(
            RetiredSourceRetirementBatchTombstoneV1 draft) {
        RetiredSourceRetirementBatchTombstoneV1 placeholder = copyRetiredBatch(draft, PLACEHOLDER);
        return copyRetiredBatch(draft, calculateRetiredBatchSha256(placeholder));
    }

    public static CanonicalBytes encodeRetiredBatch(RetiredSourceRetirementBatchTombstoneV1 value) {
        if (!value.tombstoneCanonicalSha256().equals(calculateRetiredBatchSha256(value))) {
            throw new IllegalArgumentException("retired batch tombstone identity differs from its body");
        }
        return encode(out -> {
            preamble(out, RETIRED_BATCH_MAGIC);
            writeRetiredBatchBody(out, value);
            writeDigest(out, value.tombstoneCanonicalSha256());
        });
    }

    public static RetiredSourceRetirementBatchTombstoneV1 decodeRetiredBatch(CanonicalBytes encoded) {
        RetiredSourceRetirementBatchTombstoneV1 value = decode(encoded, input -> {
            requirePreamble(input, RETIRED_BATCH_MAGIC);
            return new RetiredSourceRetirementBatchTombstoneV1(
                    enumValue(BatchMetadataStateV1.values(), input.readUnsignedByte(), "batch state"),
                    readBinding(input),
                    readDigest(input),
                    readDigest(input),
                    readDigest(input),
                    readVersion(input),
                    readDigest(input),
                    readCapability(input),
                    readDigest(input));
        });
        requireCanonical(encoded, encodeRetiredBatch(value));
        return value;
    }

    public static Sha256Digest calculateRetiredBatchSha256(RetiredSourceRetirementBatchTombstoneV1 value) {
        return Sha256Digest.hash(encode(out -> writeRetiredBatchBody(out, value)));
    }

    public static RetiredTopicIncarnationTombstoneV1 finalizeRetiredPulsar(RetiredTopicIncarnationTombstoneV1 draft) {
        RetiredTopicIncarnationTombstoneV1 placeholder = copyRetiredPulsar(draft, PLACEHOLDER);
        return copyRetiredPulsar(draft, calculateRetiredPulsarSha256(placeholder));
    }

    public static CanonicalBytes encodeRetiredPulsar(RetiredTopicIncarnationTombstoneV1 value) {
        if (!value.tombstoneCanonicalSha256().equals(calculateRetiredPulsarSha256(value))) {
            throw new IllegalArgumentException("Pulsar tombstone identity differs from its canonical body");
        }
        return encode(out -> {
            preamble(out, RETIRED_PULSAR_MAGIC);
            writeRetiredPulsarBody(out, value);
            writeDigest(out, value.tombstoneCanonicalSha256());
        });
    }

    public static RetiredTopicIncarnationTombstoneV1 decodeRetiredPulsar(CanonicalBytes encoded) {
        RetiredTopicIncarnationTombstoneV1 value = decode(encoded, input -> {
            requirePreamble(input, RETIRED_PULSAR_MAGIC);
            return new RetiredTopicIncarnationTombstoneV1(
                    readPulsarIncarnation(input),
                    new TopicBindingId(readDigest(input)),
                    readDigest(input),
                    readDigest(input),
                    input.readLong(),
                    enumValue(
                            PulsarTopicGenerationSelectorStateV1.values(),
                            input.readUnsignedByte(),
                            "Pulsar selector state"),
                    readVersion(input),
                    readVersion(input),
                    readDigest(input),
                    readCapability(input),
                    readDigest(input));
        });
        requireCanonical(encoded, encodeRetiredPulsar(value));
        return value;
    }

    public static Sha256Digest calculateRetiredPulsarSha256(RetiredTopicIncarnationTombstoneV1 value) {
        return Sha256Digest.hash(encode(out -> writeRetiredPulsarBody(out, value)));
    }

    private static void writeSnapshotBody(DataOutputStream out, RetentionFloorSnapshotV1 value) throws IOException {
        writeEnvelope(out, value.identity());
        out.writeByte(value.domain().ordinal() + 1);
        out.writeLong(value.generation());
        out.writeLong(value.priorTrimFrontier());
        writeDigest(out, value.retentionPolicyRootSha256());
        writeAuthorityFact(out, value.ownerFence());
        writeAuthorityFact(out, value.storageFence());
        out.writeInt(value.pageCount());
        out.writeLong(value.scannedBytes());
        out.writeInt(value.rows().size());
        for (RetentionFloorObservationV1 row : value.rows()) {
            writeFloor(out, row);
        }
    }

    private static void writeProofBody(DataOutputStream out, ReferenceFreeProofV1 value) throws IOException {
        writeEnvelope(out, value.identity());
        out.writeByte(value.targetKind().ordinal() + 1);
        writeDigest(out, value.targetIdentitySha256());
        writeCoverage(out, value.coverage());
        writeAuthorityFact(out, value.selectorRoot());
        writeAuthorityFact(out, value.manifestRoot());
        writeAuthorityFact(out, value.trimRoot());
        writeDigest(out, value.retentionSnapshotRootSha256());
        writeDigest(out, value.observationsRootSha256());
        out.writeInt(value.m4Releases().size());
        for (M4ReleaseBindingV1 release : value.m4Releases()) {
            writeRelease(out, release);
        }
        writeAuthorityFact(out, value.ownerFence());
        writeAuthorityFact(out, value.workerFence());
        writeAuthorityFact(out, value.storageFence());
        writeAuthorityFact(out, value.providerFence());
        out.writeLong(value.auditGraceDeadlineMillis());
        out.writeLong(value.observedAuthorityTimeMillis());
        out.writeInt(value.scanSummaries().size());
        for (ReferenceScanSummaryV1 summary : value.scanSummaries()) {
            writeSummary(out, summary);
        }
        out.writeInt(value.observations().size());
        for (ReferenceObservationV1 observation : value.observations()) {
            writeObservation(out, observation);
        }
    }

    private static ReferenceFreeProofV1 copyProof(ReferenceFreeProofV1 value, Sha256Digest proofSha) {
        return new ReferenceFreeProofV1(
                value.identity(),
                value.targetKind(),
                value.targetIdentitySha256(),
                value.coverage(),
                value.selectorRoot(),
                value.manifestRoot(),
                value.trimRoot(),
                value.retentionSnapshotRootSha256(),
                value.observationsRootSha256(),
                value.m4Releases(),
                value.ownerFence(),
                value.workerFence(),
                value.storageFence(),
                value.providerFence(),
                value.auditGraceDeadlineMillis(),
                value.observedAuthorityTimeMillis(),
                value.scanSummaries(),
                value.observations(),
                proofSha);
    }

    private static void writeRetiredBatchBody(DataOutputStream out, RetiredSourceRetirementBatchTombstoneV1 value)
            throws IOException {
        out.writeByte(value.state().ordinal() + 1);
        writeBinding(out, value.binding());
        writeDigest(out, value.batchIdSha256());
        writeDigest(out, value.fullBatchSha256());
        writeDigest(out, value.referenceFreeProofSha256());
        writeVersion(out, value.fullPredecessorVersion());
        writeDigest(out, value.fullPredecessorValueSha256());
        writeCapability(out, value.capability());
    }

    private static RetiredSourceRetirementBatchTombstoneV1 copyRetiredBatch(
            RetiredSourceRetirementBatchTombstoneV1 value, Sha256Digest canonicalSha) {
        return new RetiredSourceRetirementBatchTombstoneV1(
                value.state(),
                value.binding(),
                value.batchIdSha256(),
                value.fullBatchSha256(),
                value.referenceFreeProofSha256(),
                value.fullPredecessorVersion(),
                value.fullPredecessorValueSha256(),
                value.capability(),
                canonicalSha);
    }

    private static void writeRetiredPulsarBody(DataOutputStream out, RetiredTopicIncarnationTombstoneV1 value)
            throws IOException {
        writePulsarIncarnation(out, value.incarnation());
        writeDigest(out, value.bindingId().digest());
        writeDigest(out, value.originalAggregateSha256());
        writeDigest(out, value.referenceFreeProofSha256());
        out.writeLong(value.selectorGeneration());
        out.writeByte(value.selectorState().ordinal() + 1);
        writeVersion(out, value.selectorVersion());
        writeVersion(out, value.aggregatePredecessorVersion());
        writeDigest(out, value.aggregatePredecessorValueSha256());
        writeCapability(out, value.capability());
    }

    private static RetiredTopicIncarnationTombstoneV1 copyRetiredPulsar(
            RetiredTopicIncarnationTombstoneV1 value, Sha256Digest canonicalSha) {
        return new RetiredTopicIncarnationTombstoneV1(
                value.incarnation(),
                value.bindingId(),
                value.originalAggregateSha256(),
                value.referenceFreeProofSha256(),
                value.selectorGeneration(),
                value.selectorState(),
                value.selectorVersion(),
                value.aggregatePredecessorVersion(),
                value.aggregatePredecessorValueSha256(),
                value.capability(),
                canonicalSha);
    }

    private static void writeFloor(DataOutputStream out, RetentionFloorObservationV1 row) throws IOException {
        out.writeByte(row.floorClass().ordinal() + 1);
        writeAuthorityFact(out, row.authority());
        out.writeByte(row.domain().ordinal() + 1);
        out.writeLong(row.safeFloor());
        out.writeBoolean(row.constraining());
        out.writeBoolean(row.enumerationComplete());
    }

    private static RetentionFloorObservationV1 readFloor(DataInputStream input) throws IOException {
        return new RetentionFloorObservationV1(
                enumValue(FloorClassV1.values(), input.readUnsignedByte(), "floor class"),
                readAuthorityFact(input),
                enumValue(PositionDomain.values(), input.readUnsignedByte(), "Position Domain"),
                input.readLong(),
                input.readBoolean(),
                input.readBoolean());
    }

    private static void writeRelease(DataOutputStream out, M4ReleaseBindingV1 release) throws IOException {
        writeDigest(out, release.sourceIdentitySha256());
        out.writeLong(release.protectionGeneration());
        writeAuthorityFact(out, release.protectionAuthority());
        writeBytes(out, release.canonicalProtectionBytes(), M5RetentionRecordsV1.MAX_FULL_BATCH_BYTES);
        writeDigest(out, release.releasedByBatchSha256());
        writeDigest(out, release.releaseProofHeadSha256());
    }

    private static M4ReleaseBindingV1 readRelease(DataInputStream input) throws IOException {
        return new M4ReleaseBindingV1(
                readDigest(input),
                input.readLong(),
                readAuthorityFact(input),
                readBytes(input, M5RetentionRecordsV1.MAX_FULL_BATCH_BYTES),
                readDigest(input),
                readDigest(input));
    }

    private static void writeSummary(DataOutputStream out, ReferenceScanSummaryV1 summary) throws IOException {
        out.writeByte(summary.kind().ordinal() + 1);
        out.writeInt(summary.rowCount());
        out.writeInt(summary.pageCount());
        out.writeLong(summary.scannedBytes());
        out.writeBoolean(summary.complete());
    }

    private static ReferenceScanSummaryV1 readSummary(DataInputStream input) throws IOException {
        return new ReferenceScanSummaryV1(
                enumValue(ReferenceKindV1.values(), input.readUnsignedByte(), "reference kind"),
                input.readInt(),
                input.readInt(),
                input.readLong(),
                input.readBoolean());
    }

    private static void writeObservation(DataOutputStream out, ReferenceObservationV1 row) throws IOException {
        out.writeByte(row.kind().ordinal() + 1);
        writeAuthorityFact(out, row.authority());
        writeDigest(out, row.targetIdentitySha256());
        writeCoverage(out, row.coverage());
        out.writeByte(row.disposition().ordinal() + 1);
        out.writeBoolean(row.enumerationComplete());
    }

    private static ReferenceObservationV1 readObservation(DataInputStream input) throws IOException {
        return new ReferenceObservationV1(
                enumValue(ReferenceKindV1.values(), input.readUnsignedByte(), "reference kind"),
                readAuthorityFact(input),
                readDigest(input),
                readCoverage(input),
                enumValue(ReferenceDispositionV1.values(), input.readUnsignedByte(), "reference disposition"),
                input.readBoolean());
    }

    private static void writeAuthorityFact(DataOutputStream out, AuthorityFactV1 value) throws IOException {
        writeText(out, value.key());
        writeVersion(out, value.metadataVersion());
        writeDigest(out, value.valueSha256());
    }

    private static AuthorityFactV1 readAuthorityFact(DataInputStream input) throws IOException {
        return new AuthorityFactV1(readText(input), readVersion(input), readDigest(input));
    }

    private static void writeVersion(DataOutputStream out, MetadataVersion version) throws IOException {
        writeBytes(out, version.value(), MAX_VERSION_BYTES);
    }

    private static MetadataVersion readVersion(DataInputStream input) throws IOException {
        return new MetadataVersion(readBytes(input, MAX_VERSION_BYTES));
    }

    private static void writeEnvelope(DataOutputStream out, IdentityEnvelope value) throws IOException {
        writeDigest(out, value.protocolCellSha256());
        writeDigest(out, value.providerScopeSha256());
        writeBinding(out, value.binding());
        out.writeLong(value.ownerEpoch());
        out.writeLong(value.workerEpoch());
        out.writeLong(value.storageFence());
        writeCapability(out, value.capability());
    }

    private static IdentityEnvelope readEnvelope(DataInputStream input) throws IOException {
        return new IdentityEnvelope(
                readDigest(input),
                readDigest(input),
                readBinding(input),
                input.readLong(),
                input.readLong(),
                input.readLong(),
                readCapability(input));
    }

    private static void writeBinding(DataOutputStream out, BindingIdentity value) throws IOException {
        writeDigest(out, value.bindingId().digest());
        writeDigest(out, value.incarnationSha256());
        writeDigest(out, value.storageEpochSha256());
    }

    private static BindingIdentity readBinding(DataInputStream input) throws IOException {
        return new BindingIdentity(new TopicBindingId(readDigest(input)), readDigest(input), readDigest(input));
    }

    private static void writeCapability(DataOutputStream out, CapabilityBinding value) throws IOException {
        out.writeLong(value.generation());
        writeDigest(out, value.evidenceSha256());
    }

    private static CapabilityBinding readCapability(DataInputStream input) throws IOException {
        return new CapabilityBinding(input.readLong(), readDigest(input));
    }

    private static void writeCoverage(DataOutputStream out, ProtocolCoverage value) throws IOException {
        out.writeByte(value.domain().ordinal() + 1);
        out.writeLong(value.inclusiveStart());
        out.writeLong(value.exclusiveEnd());
    }

    private static ProtocolCoverage readCoverage(DataInputStream input) throws IOException {
        return new ProtocolCoverage(
                enumValue(PositionDomain.values(), input.readUnsignedByte(), "Position Domain"),
                input.readLong(),
                input.readLong());
    }

    private static void writePulsarIncarnation(DataOutputStream out, PulsarTopicIncarnationIdentity value)
            throws IOException {
        writeBytes(out, value.persistenceName().value().bytes(), MAX_TEXT_BYTES);
        writeBytes(out, value.topicName().value().bytes(), MAX_TEXT_BYTES);
        out.writeLong(value.bindingGeneration().value());
    }

    private static PulsarTopicIncarnationIdentity readPulsarIncarnation(DataInputStream input) throws IOException {
        return new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromBytes(readBytes(input, MAX_TEXT_BYTES).toByteArray()),
                PulsarTopicName.fromBytes(readBytes(input, MAX_TEXT_BYTES).toByteArray()),
                new PulsarBindingGeneration(input.readLong()));
    }

    private static void preamble(DataOutputStream out, int magic) throws IOException {
        out.writeInt(magic);
        out.writeInt(VERSION);
    }

    private static void requirePreamble(DataInputStream input, int magic) throws IOException {
        if (input.readInt() != magic || input.readInt() != VERSION) {
            throw new IllegalArgumentException("M5 retention value magic/version differs");
        }
    }

    private static void writeDigest(DataOutputStream out, Sha256Digest value) throws IOException {
        M5RetentionRecordsV1.requireDigest(value, "digest");
        out.write(value.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("truncated SHA-256 digest");
        }
        return Sha256Digest.copyOf(bytes);
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        writeBytes(out, CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)), MAX_TEXT_BYTES);
    }

    private static String readText(DataInputStream input) throws IOException {
        return CanonicalUtf8.fromBytes(readBytes(input, MAX_TEXT_BYTES).toByteArray())
                .value();
    }

    private static void writeBytes(DataOutputStream out, CanonicalBytes value, int maximum) throws IOException {
        if (value.isEmpty() || value.length() > maximum) {
            throw new IllegalArgumentException("canonical byte field length is outside its hard cap");
        }
        out.writeInt(value.length());
        out.write(value.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream input, int maximum) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > maximum) {
            throw new IllegalArgumentException("canonical byte field length is outside its hard cap");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("truncated canonical byte field");
        }
        return CanonicalBytes.copyOf(bytes);
    }

    private static int boundedCount(int count, int maximum, String label) {
        if (count <= 0 || count > maximum) {
            throw new IllegalArgumentException(label + " count is outside its hard cap");
        }
        return count;
    }

    private static int boundedOptionalCount(int count, int maximum, String label) {
        if (count < 0 || count > maximum) {
            throw new IllegalArgumentException(label + " count is outside its hard cap");
        }
        return count;
    }

    private static <T extends Enum<T>> T enumValue(T[] values, int encoded, String label) {
        if (encoded <= 0 || encoded > values.length) {
            throw new IllegalArgumentException(label + " code is unknown");
        }
        return values[encoded - 1];
    }

    private static CanonicalBytes encode(Encoder encoder) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                encoder.write(output);
            }
            if (bytes.size() <= 0 || bytes.size() > MAX_CONTROL_BYTES) {
                throw new IllegalArgumentException("M5 retention value length is outside its hard cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory M5 retention encoding failed", failure);
        }
    }

    private static <T> T decode(CanonicalBytes encoded, Decoder<T> decoder) {
        if (encoded == null || encoded.isEmpty() || encoded.length() > MAX_CONTROL_BYTES) {
            throw new IllegalArgumentException("M5 retention value length is outside its hard cap");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            T value = decoder.read(input);
            if (input.read() != -1) {
                throw new IllegalArgumentException("M5 retention value has trailing bytes");
            }
            return value;
        } catch (EOFException failure) {
            throw new IllegalArgumentException("M5 retention value is truncated", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("M5 retention value cannot be decoded", failure);
        }
    }

    private static void requireCanonical(CanonicalBytes encoded, CanonicalBytes canonical) {
        if (!encoded.equals(canonical)) {
            throw new IllegalArgumentException("M5 retention value is not canonical");
        }
    }

    @FunctionalInterface
    private interface Encoder {
        void write(DataOutputStream output) throws IOException;
    }

    @FunctionalInterface
    private interface Decoder<T> {
        T read(DataInputStream input) throws IOException;
    }
}
