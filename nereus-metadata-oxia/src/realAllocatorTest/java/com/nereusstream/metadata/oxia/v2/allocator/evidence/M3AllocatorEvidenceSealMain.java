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

import com.nereusstream.domain.registry.allocator.AllocatorEvidenceAttachmentKindV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceAttachmentV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceEvaluationV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceSourceArtifactsV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceSourceTupleV1;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceWriterV1;
import com.nereusstream.domain.registry.allocator.AllocatorSelectionReceiptV1;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

/** Post-JUnit fail-closed NAEA1 TEST sealer and NARS1 production-parser executor. */
public final class M3AllocatorEvidenceSealMain {
    private static final String SELECTION_FILE = "selection.nars";
    private static final String EVALUATION_FILE = "evaluation.json";

    private M3AllocatorEvidenceSealMain() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 9) {
            throw new IllegalArgumentException(
                    "expected output, JUnit XML, Oxia client, evidence, domain, SPI, Oxia, locks, executor paths");
        }
        Path output = absolute(arguments[0]);
        Path junitXml = absolute(arguments[1]);
        Path oxiaClientJar = absolute(arguments[2]);
        Path testedEvidenceArtifact = absolute(arguments[3]);
        Path runtimeDomainArtifact = absolute(arguments[4]);
        Path runtimeMetadataSpiArtifact = absolute(arguments[5]);
        Path runtimeMetadataOxiaArtifact = absolute(arguments[6]);
        Path sourceLocks = absolute(arguments[7]);
        Path executorManifest = absolute(arguments[8]);

        Path testAttachment = output.resolve(AllocatorEvidenceAttachmentKindV1.TEST.fileName());
        Path selection = output.resolve(SELECTION_FILE);
        Path evaluationFile = output.resolve(EVALUATION_FILE);
        requireAbsent(testAttachment, selection, evaluationFile);

        Path nativeAttachment = output.resolve(AllocatorEvidenceAttachmentKindV1.NATIVE.fileName());
        AllocatorEvidenceSourceTupleV1 sourceTuple =
                AllocatorEvidenceAttachmentV1.parseCanonical(nativeAttachment).sourceTuple();
        AllocatorRawEvidenceWriterV1.writeJUnitReport(testAttachment, sourceTuple, junitXml);

        List<Path> attachments = Arrays.stream(AllocatorEvidenceAttachmentKindV1.values())
                .map(kind -> output.resolve(kind.fileName()))
                .toList();
        AllocatorEvidenceSourceArtifactsV1 sourceArtifacts = new AllocatorEvidenceSourceArtifactsV1(
                oxiaClientJar,
                testedEvidenceArtifact,
                runtimeDomainArtifact,
                runtimeMetadataSpiArtifact,
                runtimeMetadataOxiaArtifact,
                sourceLocks,
                executorManifest);
        AllocatorEvidenceEvaluationV1 evaluation =
                AllocatorSelectionReceiptV1.evaluateCanonicalAttachments(attachments, sourceArtifacts);
        if (!evaluation.selectionEligible()) {
            writeEvaluation(evaluationFile, evaluation, null);
            throw new IllegalStateException(
                    "formal allocator evidence selected neither mode or qualified both: " + evaluation.status());
        }

        AllocatorSelectionReceiptV1.writeCanonicalFromEvidence(selection, attachments, sourceArtifacts);
        AllocatorSelectionReceiptV1 parsed =
                AllocatorSelectionReceiptV1.parseCanonical(selection, attachments, sourceArtifacts);
        if (!parsed.sourceTuple().equals(sourceTuple)
                || parsed.completeScaleMetrics().size() != 8
                || evaluation.selectedCandidate().isEmpty()
                || parsed.selectedMode() != evaluation.selectedCandidate().orElseThrow().mode()
                || parsed.selectedRangeSize()
                        != evaluation.selectedCandidate().orElseThrow().rangeSize()) {
            throw new IllegalStateException("sealed NARS1 differs from the production raw recomputation");
        }
        writeEvaluation(evaluationFile, evaluation, parsed);
    }

    private static void writeEvaluation(
            Path target,
            AllocatorEvidenceEvaluationV1 evaluation,
            AllocatorSelectionReceiptV1 selection) throws Exception {
        StringBuilder json = new StringBuilder(1_024);
        json.append("{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_SEAL_V1\",\"status\":\"")
                .append(selection == null ? "NON_PROMOTABLE_NO_SELECTION" : "SEALED_SELECTED")
                .append("\",\"evaluationStatus\":\"")
                .append(evaluation.status())
                .append("\",\"selectionEligible\":")
                .append(evaluation.selectionEligible())
                .append(",\"selectedMode\":");
        if (selection == null) {
            json.append("null,\"selectedRangeSize\":null,\"selectionSha256\":null");
        } else {
            json.append('\"')
                    .append(selection.selectedMode())
                    .append("\",\"selectedRangeSize\":")
                    .append(selection.selectedRangeSize())
                    .append(",\"selectionSha256\":\"")
                    .append(selection.evidenceReceiptSha256().toHex())
                    .append('\"');
        }
        AllocatorEvidenceSourceTupleV1 tuple = evaluation.sourceTuple();
        json.append(",\"source\":{\"nereusCommit\":\"")
                .append(tuple.nereusSourceCommit())
                .append("\",\"pulsarCommit\":\"")
                .append(tuple.pulsarSourceCommit())
                .append("\",\"oxiaClientCommit\":\"")
                .append(tuple.oxiaClientSourceCommit())
                .append("\",\"oxiaServerCommit\":\"")
                .append(tuple.oxiaServerSourceCommit())
                .append("\",\"sourceLocksSha256\":\"")
                .append(tuple.sourceLocksSha256().toHex())
                .append("\",\"executorManifestSha256\":\"")
                .append(tuple.executorManifestSha256().toHex())
                .append("\"},\"junit\":{\"tests\":")
                .append(evaluation.tests())
                .append(",\"failures\":")
                .append(evaluation.testFailures())
                .append(",\"errors\":")
                .append(evaluation.testErrors())
                .append(",\"skips\":")
                .append(evaluation.testSkips())
                .append("},\"attachments\":{");
        boolean first = true;
        for (AllocatorEvidenceAttachmentKindV1 kind : AllocatorEvidenceAttachmentKindV1.values()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append('\"')
                    .append(kind.fileName())
                    .append("\":\"")
                    .append(evaluation.attachmentSha256().get(kind).toHex())
                    .append('\"');
        }
        json.append("}}\n");
        Files.writeString(
                target,
                json.toString(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
    }

    private static Path absolute(String value) {
        Path path = Path.of(value).toAbsolutePath().normalize();
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("allocator evidence path is not absolute");
        }
        return path;
    }

    private static void requireAbsent(Path... paths) {
        for (Path path : paths) {
            if (Files.exists(path)) {
                throw new IllegalStateException("allocator evidence output already exists: " + path);
            }
        }
    }
}
