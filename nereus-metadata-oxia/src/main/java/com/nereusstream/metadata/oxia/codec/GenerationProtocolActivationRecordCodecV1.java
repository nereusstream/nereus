/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.util.ArrayList;
import java.util.List;

/** Strict binary-v1 codec for the cluster generation activation authority. */
public final class GenerationProtocolActivationRecordCodecV1
        extends AbstractF4RecordCodecV1<GenerationProtocolActivationRecord> {
    public GenerationProtocolActivationRecordCodecV1() {
        super(GenerationProtocolActivationRecord.class);
    }

    @Override
    public byte[] encode(GenerationProtocolActivationRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeInt(value.protocolVersion());
            writer.writeInt(value.lifecycle().wireId());
            writer.writeOptional(value.publicationEnabled());
            writer.writeOptional(value.physicalDeleteEnabled());
            writer.writeOptional(value.cursorSnapshotDeleteEnabled());
            writer.writeLong(value.brokerCapabilityReadinessEpoch());
            writer.writeInt(value.requiredReferenceDomains().size());
            for (ReferenceDomainVersionRecord domain : value.requiredReferenceDomains()) {
                writer.writeString(domain.domainId());
                writer.writeInt(domain.protocolVersion());
            }
            writeBackfill(writer, value.streamRegistrationBackfill());
            writeBackfill(writer, value.physicalRootBackfill());
            writeBackfill(writer, value.cursorSnapshotBackfill());
            writer.writeString(value.objectStoreCapabilitySha256());
            writer.writeString(value.activatingBrokerRunId());
            writer.writeLong(value.preparedAtMillis());
            writer.writeLong(value.activatedAtMillis());
            writer.writeLong(value.updatedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public GenerationProtocolActivationRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            int protocolVersion = reader.readInt("protocolVersion");
            GenerationProtocolActivationLifecycle lifecycle =
                    GenerationProtocolActivationLifecycle.fromWireId(
                            reader.readInt("lifecycle"));
            boolean publicationEnabled = reader.readOptional("publicationEnabled");
            boolean physicalDeleteEnabled = reader.readOptional("physicalDeleteEnabled");
            boolean cursorSnapshotDeleteEnabled =
                    reader.readOptional("cursorSnapshotDeleteEnabled");
            long readinessEpoch = reader.readLong("brokerCapabilityReadinessEpoch");
            int domainCount = reader.readCount(
                    "requiredReferenceDomainCount",
                    Integer.BYTES * 2,
                    GenerationProtocolActivationRecord.MAX_REFERENCE_DOMAINS);
            List<ReferenceDomainVersionRecord> domains = new ArrayList<>(domainCount);
            for (int index = 0; index < domainCount; index++) {
                domains.add(new ReferenceDomainVersionRecord(
                        reader.readString("domainId"),
                        reader.readInt("domainProtocolVersion")));
            }
            GenerationProtocolActivationRecord value =
                    new GenerationProtocolActivationRecord(
                            VERSION,
                            protocolVersion,
                            lifecycle,
                            publicationEnabled,
                            physicalDeleteEnabled,
                            cursorSnapshotDeleteEnabled,
                            readinessEpoch,
                            domains,
                            readBackfill(reader, "streamRegistrationBackfill"),
                            readBackfill(reader, "physicalRootBackfill"),
                            readBackfill(reader, "cursorSnapshotBackfill"),
                            reader.readString("objectStoreCapabilitySha256"),
                            reader.readString("activatingBrokerRunId"),
                            reader.readLong("preparedAtMillis"),
                            reader.readLong("activatedAtMillis"),
                            reader.readLong("updatedAtMillis"),
                            reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    private static void writeBackfill(
            F4Binary.Writer writer,
            GenerationBackfillProofRecord proof) {
        writer.writeString(proof.runId());
        writer.writeLong(proof.brokerReadinessEpoch());
        writer.writeString(proof.coverageSha256());
        writer.writeOptional(proof.complete());
        writer.writeLong(proof.completedAtMillis());
    }

    private static GenerationBackfillProofRecord readBackfill(
            F4Binary.Reader reader,
            String name) {
        return new GenerationBackfillProofRecord(
                reader.readString(name + "RunId"),
                reader.readLong(name + "BrokerReadinessEpoch"),
                reader.readString(name + "CoverageSha256"),
                reader.readOptional(name + "Complete"),
                reader.readLong(name + "CompletedAtMillis"));
    }
}
