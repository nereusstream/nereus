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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceAttachmentKindV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceSourceTupleV1;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceWriterV1;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/** Owns the four streaming raw attachments; the JUnit attachment is sealed only after Gradle writes its XML. */
final class M3AllocatorRawEvidenceFiles implements AutoCloseable {
    private final AllocatorRawEvidenceWriterV1 nativeWriter;
    private final AllocatorRawEvidenceWriterV1 faultWriter;
    private final AllocatorRawEvidenceWriterV1 scale10kWriter;
    private final AllocatorRawEvidenceWriterV1 scale100kWriter;
    private final LongAdder nativeEvents = new LongAdder();
    private final LongAdder faultEvents = new LongAdder();
    private final LongAdder scale10kEvents = new LongAdder();
    private final LongAdder scale100kEvents = new LongAdder();

    M3AllocatorRawEvidenceFiles(Path outputDirectory, AllocatorEvidenceSourceTupleV1 sourceTuple) {
        Objects.requireNonNull(outputDirectory, "outputDirectory");
        Objects.requireNonNull(sourceTuple, "sourceTuple");
        try {
            Files.createDirectories(outputDirectory);
        } catch (java.io.IOException error) {
            throw new IllegalStateException("allocator raw evidence directory could not be created", error);
        }
        nativeWriter = open(outputDirectory, "native.naea", AllocatorEvidenceAttachmentKindV1.NATIVE, sourceTuple);
        faultWriter = open(outputDirectory, "fault.naea", AllocatorEvidenceAttachmentKindV1.FAULT, sourceTuple);
        scale10kWriter =
                open(outputDirectory, "scale-10000.naea", AllocatorEvidenceAttachmentKindV1.SCALE_10K, sourceTuple);
        scale100kWriter = open(
                outputDirectory, "scale-100000.naea", AllocatorEvidenceAttachmentKindV1.SCALE_100K, sourceTuple);
    }

    M3AllocatorRequestTelemetry.EventSink candidateSink() {
        return event -> {
            if (event.context().nativePath()) {
                throw new IllegalArgumentException("native event cannot enter candidate scale evidence");
            }
            if (event.context().activeManagedLedgers() == 10_000) {
                scale10kWriter.append(event);
                scale10kEvents.increment();
            } else if (event.context().activeManagedLedgers() == 100_000) {
                scale100kWriter.append(event);
                scale100kEvents.increment();
            } else {
                throw new IllegalArgumentException("candidate event has an unfrozen population");
            }
        };
    }

    M3AllocatorRequestTelemetry.EventSink nativeSink() {
        return event -> {
            if (!event.context().nativePath()) {
                throw new IllegalArgumentException("candidate event cannot enter native evidence");
            }
            nativeWriter.append(event);
            nativeEvents.increment();
        };
    }

    M3AllocatorRequestTelemetry.EventSink faultSink() {
        return event -> {
            if (event.context().nativePath()
                    || (event.flags() & AllocatorRawEvidenceEventV1.FLAG_FAULT_CUT_MASK) == 0) {
                throw new IllegalArgumentException("fault evidence requires one exact candidate fault cut");
            }
            faultWriter.append(event);
            faultEvents.increment();
        };
    }

    EvidenceCounts counts() {
        return new EvidenceCounts(
                nativeEvents.sum(), faultEvents.sum(), scale10kEvents.sum(), scale100kEvents.sum());
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        for (AllocatorRawEvidenceWriterV1 writer :
                java.util.List.of(scale100kWriter, scale10kWriter, faultWriter, nativeWriter)) {
            try {
                writer.close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    static AllocatorEvidenceSourceTupleV1 sourceTupleFromSystemProperties() {
        return new AllocatorEvidenceSourceTupleV1(
                requiredProperty("nereusSourceCommit"),
                requiredProperty("pulsarSourceCommit"),
                requiredProperty("oxiaClientSourceCommit"),
                requiredProperty("oxiaServerSourceCommit"),
                digestProperty("oxiaClientJarSha256"),
                digestProperty("testedEvidenceArtifactSha256"),
                digestProperty("runtimeDomainArtifactSha256"),
                digestProperty("runtimeMetadataSpiArtifactSha256"),
                digestProperty("runtimeMetadataOxiaArtifactSha256"),
                digestProperty("sourceLocksSha256"),
                digestProperty("executorManifestSha256"));
    }

    private static AllocatorRawEvidenceWriterV1 open(
            Path directory,
            String fileName,
            AllocatorEvidenceAttachmentKindV1 kind,
            AllocatorEvidenceSourceTupleV1 sourceTuple) {
        return AllocatorRawEvidenceWriterV1.open(directory.resolve(fileName), kind, sourceTuple);
    }

    private static Sha256Digest digestProperty(String suffix) {
        byte[] value;
        try {
            value = HexFormat.of().parseHex(requiredProperty(suffix));
        } catch (IllegalArgumentException error) {
            throw new IllegalStateException("allocator evidence digest property is not hexadecimal: " + suffix, error);
        }
        return Sha256Digest.copyOf(value);
    }

    static String requiredProperty(String suffix) {
        String value = System.getProperty("nereus.m3.allocator." + suffix);
        if (value == null || value.isBlank() || "UNSET".equals(value)) {
            throw new IllegalStateException("missing allocator evidence property " + suffix);
        }
        return value;
    }

    record EvidenceCounts(long nativeEvents, long faultEvents, long scale10kEvents, long scale100kEvents) {
        EvidenceCounts {
            if (nativeEvents < 0 || faultEvents < 0 || scale10kEvents < 0 || scale100kEvents < 0) {
                throw new IllegalArgumentException("allocator evidence event counts cannot be negative");
            }
        }
    }
}
