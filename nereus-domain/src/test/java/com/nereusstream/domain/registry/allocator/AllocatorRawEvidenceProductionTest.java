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

package com.nereusstream.domain.registry.allocator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AllocatorRawEvidenceProductionTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void rawEventRoundTripRequiresCanonicalTrailingZeroAndClosedFlagLayout() {
        AllocatorEvidenceContextV1 context = AllocatorEvidenceContextV1.nativeContext(10_000, 1, 200);
        AllocatorRawEvidenceEventV1 event = new AllocatorRawEvidenceEventV1(
                context,
                AllocatorRawEvidenceEventV1.EventKind.OFFERED,
                AllocatorEvidenceScheduleV1.actorId(0),
                AllocatorEvidenceScheduleV1.trigger(0),
                AllocatorRawEvidenceEventV1.EventOutcome.NONE,
                AllocatorRawEvidenceEventV1.flags(null, null, 0, true),
                0,
                AllocatorEvidenceScheduleV1.ledgerCursor(10_000).nextLedgerIndex(),
                0,
                0,
                0,
                0,
                0);

        assertThat(AllocatorRawEvidenceEventV1.decode(
                        ByteBuffer.wrap(event.encode().toByteArray())))
                .isEqualTo(event);
        byte[] changed = event.encode().toByteArray();
        changed[changed.length - 1] = 1;
        assertThatThrownBy(() -> AllocatorRawEvidenceEventV1.decode(ByteBuffer.wrap(changed)))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE));
        assertThatThrownBy(() -> new AllocatorEvidenceContextV1(0, true, null, 100_000, 1, 200))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("aliases different frozen dimensions");
    }

    @Test
    void queueDepthIsReplayedAndCallerCannotForgeAPassingLowMaximum() {
        AllocatorRawEvidenceValidatorV1.validateQueueDepthTransitions(
                new long[] {1, 2, 2, 3},
                new long[] {
                    AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE,
                    AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE,
                    AllocatorRawEvidenceEventV1.QUEUE_DEQUEUE,
                    AllocatorRawEvidenceEventV1.QUEUE_DEQUEUE
                },
                new long[] {1, 2, 1, 0});

        assertThatThrownBy(() -> AllocatorRawEvidenceValidatorV1.validateQueueDepthTransitions(
                        new long[] {1, 2},
                        new long[] {AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE, AllocatorRawEvidenceEventV1.QUEUE_DEQUEUE
                        },
                        new long[] {0, 0}))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE));
        assertThatThrownBy(() -> AllocatorRawEvidenceValidatorV1.validateQueueDepthTransitions(
                        new long[] {1, 1, 2, 2},
                        new long[] {
                            AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE,
                            AllocatorRawEvidenceEventV1.QUEUE_ENQUEUE,
                            AllocatorRawEvidenceEventV1.QUEUE_DEQUEUE,
                            AllocatorRawEvidenceEventV1.QUEUE_DEQUEUE
                        },
                        new long[] {1, 1, 0, 0}))
                .isInstanceOf(AllocatorProtocolException.class);
    }

    @Test
    void junitWriterRecomputesCountsFromExactSecureXml() throws Exception {
        Path xml = temporaryDirectory.resolve("allocator-tests.xml");
        Files.writeString(xml, passingXml(), StandardCharsets.UTF_8);
        Path envelope = temporaryDirectory.resolve("test.naea");

        AllocatorRawEvidenceWriterV1.writeJUnitReport(envelope, tuple("a"), xml);

        AllocatorEvidenceAttachmentV1 parsed = AllocatorEvidenceAttachmentV1.parseCanonical(envelope);
        assertThat(parsed.kind()).isEqualTo(AllocatorEvidenceAttachmentKindV1.TEST);
        assertThat(parsed.sourceTuple()).isEqualTo(tuple("a"));
        assertThat(parsed.envelopeSha256()).isEqualTo(AllocatorEvidenceSourceArtifactsV1.sha256(envelope));
        AllocatorJUnitEvidenceV1.Counts counts = AllocatorJUnitEvidenceV1.parse(
                new ByteArrayInputStream(passingXml().getBytes(StandardCharsets.UTF_8)));
        assertThat(counts).isEqualTo(new AllocatorJUnitEvidenceV1.Counts(1, 0, 0, 0));
        assertThatThrownBy(() -> AllocatorRawEvidenceWriterV1.writeJUnitReport(
                        temporaryDirectory.resolve("test-alias.naea"), tuple("a"), xml))
                .isInstanceOf(AllocatorProtocolException.class);

        try (FileChannel changed = FileChannel.open(envelope, StandardOpenOption.WRITE)) {
            changed.position(AllocatorEvidenceAttachmentV1.HEADER_BYTES + 64);
            changed.write(ByteBuffer.wrap(new byte[] {(byte) 0xff}));
        }
        assertThatThrownBy(() -> {
                    try (java.io.InputStream payload = parsed.openPayload()) {
                        payload.readAllBytes();
                    }
                })
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("changed after envelope validation");

        String forged = "<testsuite tests=\"1\" failures=\"0\" errors=\"0\" skipped=\"0\">"
                + "<testcase name=\"x\"><failure/></testcase></testsuite>";
        assertThatThrownBy(() -> AllocatorJUnitEvidenceV1.parse(
                        new ByteArrayInputStream(forged.getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(AllocatorProtocolException.class);
        String xxe = "<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]>"
                + "<testsuite tests=\"1\" failures=\"0\" errors=\"0\" skipped=\"0\">"
                + "<testcase name=\"&e;\"/></testsuite>";
        assertThatThrownBy(() ->
                        AllocatorJUnitEvidenceV1.parse(new ByteArrayInputStream(xxe.getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(AllocatorProtocolException.class);
    }

    @Test
    void productionAttachmentInventoryRejectsSourceDriftBeforePayloadInterpretation() throws Exception {
        List<Path> files = new ArrayList<>();
        for (AllocatorEvidenceAttachmentKindV1 kind : AllocatorEvidenceAttachmentKindV1.values()) {
            AllocatorEvidenceSourceTupleV1 source =
                    kind == AllocatorEvidenceAttachmentKindV1.SCALE_100K ? tuple("b") : tuple("a");
            CanonicalBytes envelope =
                    AllocatorEvidenceAttachmentV1.canonicalForTest(kind, source, CanonicalBytes.copyOf(new byte[] {1}));
            Path file = temporaryDirectory.resolve(kind.fileName());
            Files.write(file, envelope.toByteArray());
            files.add(file);
        }

        assertThatThrownBy(
                        () -> AllocatorSelectionReceiptV1.evaluateCanonicalAttachments(files, missingSourceArtifacts()))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE));
    }

    @Test
    void productionParserRejectsCallerForgedJunitHeaderCounts() throws Exception {
        byte[] xml = passingXml().getBytes(StandardCharsets.UTF_8);
        ByteBuffer payload = ByteBuffer.allocate(64 + xml.length);
        payload.put("NAJT".getBytes(StandardCharsets.US_ASCII))
                .putShort((short) 1)
                .putShort((short) 0)
                .putLong(2)
                .putLong(0)
                .putLong(0)
                .putLong(0)
                .putInt(xml.length)
                .put(new byte[20])
                .put(xml);
        List<Path> files = new ArrayList<>();
        for (AllocatorEvidenceAttachmentKindV1 kind : AllocatorEvidenceAttachmentKindV1.values()) {
            CanonicalBytes rawPayload = kind == AllocatorEvidenceAttachmentKindV1.TEST
                    ? CanonicalBytes.copyOf(payload.array())
                    : CanonicalBytes.copyOf(new byte[] {1});
            CanonicalBytes envelope = AllocatorEvidenceAttachmentV1.canonicalForTest(kind, tuple("a"), rawPayload);
            Path file = temporaryDirectory.resolve(kind.fileName());
            Files.write(file, envelope.toByteArray());
            files.add(file);
        }

        assertThatThrownBy(() -> AllocatorSelectionReceiptV1.evaluateCanonicalAttachments(files, sourceArtifacts("a")))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.getMessage())
                        .contains("caller counts"));
    }

    @Test
    void productionInventoryRequiresOneDirectoryAndFiveExactBasenames() throws Exception {
        assertThat(AllocatorSelectionReceiptV1.canonicalAttachmentPaths(temporaryDirectory))
                .extracting(path -> path.getFileName().toString())
                .containsExactly("test.naea", "native.naea", "fault.naea", "scale-10000.naea", "scale-100000.naea");

        List<Path> files = new ArrayList<>();
        for (AllocatorEvidenceAttachmentKindV1 kind : AllocatorEvidenceAttachmentKindV1.values()) {
            CanonicalBytes envelope = AllocatorEvidenceAttachmentV1.canonicalForTest(
                    kind, tuple("a"), CanonicalBytes.copyOf(new byte[] {1}));
            Path file = temporaryDirectory.resolve(
                    kind == AllocatorEvidenceAttachmentKindV1.NATIVE ? "native-alias.naea" : kind.fileName());
            Files.write(file, envelope.toByteArray());
            files.add(file);
        }

        assertThatThrownBy(() -> AllocatorSelectionReceiptV1.evaluateCanonicalAttachments(files, sourceArtifacts("a")))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.getMessage())
                        .contains("closed kind basename"));
    }

    @Test
    void sourceTupleRejectsZeroCommitSentinels() {
        assertThatThrownBy(() -> new AllocatorEvidenceSourceTupleV1(
                        "0".repeat(40),
                        "b".repeat(40),
                        "c".repeat(40),
                        "d".repeat(40),
                        digest("client"),
                        digest("evidence"),
                        digest("domain"),
                        digest("spi"),
                        digest("oxia"),
                        digest("locks"),
                        digest("executor")))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE));
    }

    private static String passingXml() {
        return "<testsuite tests=\"1\" failures=\"0\" errors=\"0\" skipped=\"0\">"
                + "<testcase name=\"allocator\"/></testsuite>";
    }

    private static AllocatorEvidenceSourceTupleV1 tuple(String prefix) {
        String commit = prefix.repeat(40);
        return new AllocatorEvidenceSourceTupleV1(
                commit,
                "b".repeat(40),
                "c".repeat(40),
                "d".repeat(40),
                digest(prefix + "-client"),
                digest(prefix + "-evidence"),
                digest(prefix + "-domain"),
                digest(prefix + "-spi"),
                digest(prefix + "-oxia"),
                digest(prefix + "-locks"),
                digest(prefix + "-executor"));
    }

    private AllocatorEvidenceSourceArtifactsV1 sourceArtifacts(String prefix) throws Exception {
        return new AllocatorEvidenceSourceArtifactsV1(
                sourceFile(prefix + "-client"),
                sourceFile(prefix + "-evidence"),
                sourceFile(prefix + "-domain"),
                sourceFile(prefix + "-spi"),
                sourceFile(prefix + "-oxia"),
                sourceFile(prefix + "-locks"),
                sourceFile(prefix + "-executor"));
    }

    private AllocatorEvidenceSourceArtifactsV1 missingSourceArtifacts() {
        Path missing = temporaryDirectory.resolve("missing");
        return new AllocatorEvidenceSourceArtifactsV1(missing, missing, missing, missing, missing, missing, missing);
    }

    private Path sourceFile(String content) throws Exception {
        Path file = temporaryDirectory.resolve("source-" + content);
        Files.writeString(file, content, StandardCharsets.UTF_8);
        return file;
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }
}
