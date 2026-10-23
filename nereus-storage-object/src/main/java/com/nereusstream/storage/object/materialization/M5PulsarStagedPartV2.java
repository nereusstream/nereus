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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** One owned repeatable staged part. Its caller still needs actual Task/Cell/Provider writer admission. */
public final class M5PulsarStagedPartV2 implements RepeatableObjectBody, AutoCloseable {
    private final Path directory;
    private final Path body;
    private final GenerationObject descriptor;
    private final List<M5LookupIndexV1.Row> locators;
    private int readers;
    private boolean closing;
    private boolean removed;

    private M5PulsarStagedPartV2(
            Path directory, Path body, GenerationObject descriptor, List<M5LookupIndexV1.Row> locators) {
        this.directory = directory;
        this.body = body;
        this.descriptor = descriptor;
        this.locators = List.copyOf(locators);
    }

    /** Numeric cap bounds local staging only; it is never proof that the caller reserved durable task capacity. */
    public static M5PulsarStagedPartV2 stage(
            MaterializationPlan plan,
            int ordinal,
            M5BytePreservingMaterializerV1.SourceReader reader,
            Path cellStagingRoot,
            long maximumStagedBytes)
            throws IOException {
        M5MaterializationCodecV1.encodePlan(plan);
        if (!plan.sourceCut().coverage().typedPulsar()
                || plan.representationMode() != M5MaterializationRecordsV1.RepresentationMode.REWRITE_GENERATION
                || plan.payloadKind() != M5MaterializationRecordsV1.PayloadKind.PULSAR_ENTRY_PRESERVING_V1
                || ordinal < 0
                || ordinal >= plan.outputParts().size()
                || maximumStagedBytes <= 0) {
            throw new IllegalArgumentException("Pulsar staged part is outside its rewrite plan/byte cap");
        }
        Files.createDirectories(cellStagingRoot);
        Path directory = Files.createTempDirectory(cellStagingRoot, "m5-pulsar-");
        Path payload = directory.resolve("payload");
        Path body = directory.resolve("body");
        try {
            var source = plan.sourceCut().sources().get(ordinal);
            reader.verifyPulsarSource(source);
            var commitment = new M5PulsarEntriesV2.Commitment(
                    source.coverage().pulsarCoverage().orElseThrow());
            MessageDigest payloadSha = sha256();
            List<Nms1ObjectV1.ExtentRow> extents = new ArrayList<>();
            List<M5LookupIndexV1.Row> locators = new ArrayList<>();
            long payloadBytes = 0;
            try (var output = new DigestOutputStream(Files.newOutputStream(payload), payloadSha);
                    var entries = reader.openPulsarEntries(source)) {
                for (var entry = entries.next(); entry.isPresent(); entry = entries.next()) {
                    var actual = entry.orElseThrow();
                    commitment.accept(actual);
                    long next = Math.addExact(payloadBytes, actual.payload().length());
                    if (extents.size() == Nms1ObjectV1.MAX_EXTENT_ROWS
                            || next > Nms1CodecV1.MAX_PAYLOAD_BYTES
                            || Math.addExact(Math.multiplyExact(2, next), Nms1CodecV1.FOOTER_BYTES)
                                    > maximumStagedBytes) {
                        throw new IOException("Pulsar staging exceeds the reserved local byte/entry bound");
                    }
                    var range = ProtocolCoverage.pulsar(source.coverage()
                            .pulsarCoverage()
                            .orElseThrow()
                            .withRanges(List.of(new M5PulsarCoverageV2.EntryRange(
                                    actual.position().ledgerId(),
                                    actual.position().entryId(),
                                    Math.addExact(actual.position().entryId(), 1)))));
                    var digest = Sha256Digest.hash(actual.payload());
                    extents.add(new Nms1ObjectV1.ExtentRow(
                            range,
                            Math.toIntExact(payloadBytes),
                            actual.payload().length(),
                            1,
                            actual.timestamp(),
                            actual.timestamp(),
                            digest,
                            0));
                    locators.add(new M5LookupIndexV1.Row(
                            range,
                            ordinal,
                            payloadBytes,
                            actual.payload().length(),
                            actual.timestamp(),
                            actual.timestamp(),
                            digest));
                    output.write(actual.payload().toByteArray());
                    payloadBytes = next;
                }
            }
            if (!commitment.finish().equals(source.pulsarEntryRootSha256().orElseThrow())) {
                throw new IOException("Pulsar staged source entry root changed");
            }
            commitment.requireTimestampSummary(source.minimumTimestamp(), source.maximumTimestamp());
            reader.verifyPulsarSource(source);
            Nms1CodecV1.StreamedBody exact;
            try (var input = Files.newInputStream(payload);
                    var output = Files.newOutputStream(body)) {
                exact = Nms1CodecV1.writePulsarPart(
                        plan,
                        ordinal,
                        extents,
                        payloadBytes,
                        Sha256Digest.copyOf(payloadSha.digest()),
                        input,
                        new CappedOutput(output, Math.subtractExact(maximumStagedBytes, payloadBytes)));
            }
            if (Math.addExact(exact.bytes(), payloadBytes) > maximumStagedBytes) {
                throw new IOException("Pulsar staged canonical body exceeds local byte capacity");
            }
            Files.delete(payload);
            var part = plan.outputParts().get(ordinal);
            var descriptor = new GenerationObject(
                    ordinal,
                    null,
                    part.coverage(),
                    new ObjectIdentity(part.objectKey(), exact.bytes(), exact.sha256()),
                    Optional.empty());
            var result = new M5PulsarStagedPartV2(directory, body, descriptor, locators);
            return result;
        } catch (IOException | RuntimeException | Error failure) {
            for (var owned : List.of(body, payload, directory)) {
                try {
                    Files.deleteIfExists(owned);
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    public GenerationObject descriptor() {
        return descriptor;
    }

    public List<M5LookupIndexV1.Row> locators() {
        return locators;
    }

    @Override
    public ObjectIdentity identity() {
        return descriptor.identity();
    }

    @Override
    public synchronized InputStream openStream() throws IOException {
        if (closing || removed) {
            throw new IOException("Pulsar staging admission is closed");
        }
        var input = Files.newInputStream(body);
        readers++;
        return new InputStream() {
            private boolean closed;

            @Override
            public synchronized int read() throws IOException {
                requireOpen();
                return input.read();
            }

            @Override
            public synchronized int read(byte[] bytes, int offset, int length) throws IOException {
                requireOpen();
                return input.read(bytes, offset, length);
            }

            private void requireOpen() throws IOException {
                if (closed) {
                    throw new IOException("Pulsar staged original stream is closed");
                }
            }

            @Override
            public synchronized void close() throws IOException {
                if (closed) {
                    return;
                }
                // Serialize with original reads, and retain the reader charge if the actual close fails.
                input.close();
                closed = true;
                synchronized (M5PulsarStagedPartV2.this) {
                    readers--;
                }
            }
        };
    }

    @Override
    public synchronized void close() throws IOException {
        closing = true;
        if (removed) {
            return;
        }
        if (readers != 0) {
            throw new IOException("Pulsar staged body has undrained original readers");
        }
        Files.deleteIfExists(body);
        Files.deleteIfExists(directory);
        removed = true;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("JDK has no SHA-256", impossible);
        }
    }

    private static final class CappedOutput extends java.io.FilterOutputStream {
        private final long maximumBytes;
        private long bytes;

        private CappedOutput(java.io.OutputStream output, long maximumBytes) {
            super(output);
            this.maximumBytes = maximumBytes;
        }

        @Override
        public void write(int value) throws IOException {
            requireBytes(1);
            out.write(value);
        }

        @Override
        public void write(byte[] value, int offset, int length) throws IOException {
            requireBytes(length);
            out.write(value, offset, length);
        }

        private void requireBytes(int count) throws IOException {
            long next = Math.addExact(bytes, count);
            if (next > maximumBytes) {
                throw new IOException("Pulsar stage write exceeds its local byte capacity");
            }
            bytes = next;
        }
    }
}
