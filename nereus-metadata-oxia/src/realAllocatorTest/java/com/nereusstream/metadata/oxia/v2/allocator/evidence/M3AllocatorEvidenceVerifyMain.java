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
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceEvaluationV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceSourceArtifactsV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceSourceTupleV1;
import com.nereusstream.domain.registry.allocator.AllocatorSelectionReceiptV1;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

/** Read-only production reparser for an already sealed NARS1 plus exact five-file NAEA1 inventory. */
public final class M3AllocatorEvidenceVerifyMain {
    static final String SELF_HASH_RULE = "SHA256_OF_EXACT_UTF8_WITH_SELF_SHA256_64_ZERO_HEX";
    private static final String ZERO_SHA = "0".repeat(64);

    private M3AllocatorEvidenceVerifyMain() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 9) {
            throw new IllegalArgumentException(
                    "expected evidence directory, seven source artifacts, and verification output");
        }
        Path evidenceDirectory = absolute(arguments[0]);
        List<Path> attachments = Arrays.stream(AllocatorEvidenceAttachmentKindV1.values())
                .map(kind -> evidenceDirectory.resolve(kind.fileName()))
                .toList();
        List<Path> sourceArtifactPaths = List.of(
                absolute(arguments[1]),
                absolute(arguments[2]),
                absolute(arguments[3]),
                absolute(arguments[4]),
                absolute(arguments[5]),
                absolute(arguments[6]),
                absolute(arguments[7]));
        AllocatorEvidenceSourceArtifactsV1 artifacts = new AllocatorEvidenceSourceArtifactsV1(
                sourceArtifactPaths.get(0),
                sourceArtifactPaths.get(1),
                sourceArtifactPaths.get(2),
                sourceArtifactPaths.get(3),
                sourceArtifactPaths.get(4),
                sourceArtifactPaths.get(5),
                sourceArtifactPaths.get(6));
        Path output = absolute(arguments[8]);
        if (!"raw-verification-payload.json".equals(output.getFileName().toString())) {
            throw new IllegalArgumentException("allocator raw verification output basename differs");
        }
        if (Files.exists(output)) {
            throw new IllegalStateException("allocator raw verification output already exists: " + output);
        }

        AllocatorEvidenceEvaluationV1 evaluation =
                AllocatorSelectionReceiptV1.evaluateCanonicalAttachments(attachments, artifacts);
        AllocatorSelectionReceiptV1 receipt = AllocatorSelectionReceiptV1.parseCanonical(
                evidenceDirectory.resolve("selection.nars"), attachments, artifacts);
        if (!evaluation.selectionEligible()
                || evaluation.selectedCandidate().isEmpty()
                || receipt.selectedMode() != evaluation.selectedCandidate().orElseThrow().mode()
                || receipt.selectedRangeSize()
                        != evaluation.selectedCandidate().orElseThrow().rangeSize()
                || receipt.completeScaleMetrics().size() != 8
                || evaluation.tests() <= 0
                || evaluation.testFailures() != 0
                || evaluation.testErrors() != 0
                || evaluation.testSkips() != 0) {
            throw new IllegalStateException("allocator raw verification did not reproduce one zero-error selection");
        }
        writeVerification(output, evaluation, receipt, attachments, sourceArtifactPaths);
    }

    private static void writeVerification(
            Path output,
            AllocatorEvidenceEvaluationV1 evaluation,
            AllocatorSelectionReceiptV1 receipt,
            List<Path> attachments,
            List<Path> sourceArtifactPaths) throws Exception {
        AllocatorEvidenceSourceTupleV1 tuple = receipt.sourceTuple();
        StringBuilder json = new StringBuilder(1_400);
        json.append("{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_RAW_RECOMPUTATION_V1\","
                        + "\"selfSha256\":\""
                        + ZERO_SHA
                        + "\",\"selfHashRule\":\""
                        + SELF_HASH_RULE
                        + "\",\"authority\":false,\"status\":\"PASS_RAW_RECOMPUTED\","
                        + "\"selectionEligible\":true,\"testedCommit\":\"")
                .append(tuple.nereusSourceCommit())
                .append("\",\"sourceLocksSha256\":\"")
                .append(tuple.sourceLocksSha256().toHex())
                .append("\","
                        + "\"selectedMode\":\"")
                .append(receipt.selectedMode())
                .append("\",\"selectedRangeSize\":")
                .append(receipt.selectedRangeSize())
                .append(",\"selection\":{\"basename\":\"selection.nars\",\"bytes\":")
                .append(AllocatorSelectionReceiptV1.RECEIPT_BYTES)
                .append(",\"sha256\":\"")
                .append(receipt.evidenceReceiptSha256().toHex())
                .append("\"},\"derived\":{\"intervals\":288,\"faultCutKinds\":")
                .append(AllocatorSelectionReceiptV1.REQUIRED_FAULT_CUTS)
                .append(",\"selectedRows\":")
                .append(receipt.completeScaleMetrics().size())
                .append("},\"junit\":{\"tests\":")
                .append(evaluation.tests())
                .append(",\"failures\":")
                .append(evaluation.testFailures())
                .append(",\"errors\":")
                .append(evaluation.testErrors())
                .append(",\"skips\":")
                .append(evaluation.testSkips())
                .append("},\"source\":{\"nereusCommit\":\"")
                .append(tuple.nereusSourceCommit())
                .append("\",\"pulsarCommit\":\"")
                .append(tuple.pulsarSourceCommit())
                .append("\",\"oxiaClientCommit\":\"")
                .append(tuple.oxiaClientSourceCommit())
                .append("\",\"oxiaServerCommit\":\"")
                .append(tuple.oxiaServerSourceCommit())
                .append("\",\"oxiaClientJarSha256\":\"")
                .append(tuple.oxiaClientJarSha256().toHex())
                .append("\",\"testedEvidenceArtifactSha256\":\"")
                .append(tuple.testedEvidenceArtifactSha256().toHex())
                .append("\",\"runtimeDomainArtifactSha256\":\"")
                .append(tuple.runtimeDomainArtifactSha256().toHex())
                .append("\",\"runtimeMetadataSpiArtifactSha256\":\"")
                .append(tuple.runtimeMetadataSpiArtifactSha256().toHex())
                .append("\",\"runtimeMetadataOxiaArtifactSha256\":\"")
                .append(tuple.runtimeMetadataOxiaArtifactSha256().toHex())
                .append("\",\"sourceLocksSha256\":\"")
                .append(tuple.sourceLocksSha256().toHex())
                .append("\",\"executorManifestSha256\":\"")
                .append(tuple.executorManifestSha256().toHex())
                .append("\"},\"attachments\":{");
        boolean first = true;
        AllocatorEvidenceAttachmentKindV1[] kinds = AllocatorEvidenceAttachmentKindV1.values();
        for (int index = 0; index < kinds.length; index++) {
            AllocatorEvidenceAttachmentKindV1 kind = kinds[index];
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append('\"')
                    .append(kind.fileName())
                    .append("\":{\"bytes\":")
                    .append(Files.size(attachments.get(index)))
                    .append(",\"envelopeSha256\":\"")
                    .append(receipt.attachmentSha256().get(kind).toHex())
                    .append("\"}");
        }
        String[] logicalNames = {
            "oxiaClientJar",
            "testedEvidenceArtifact",
            "runtimeDomainArtifact",
            "runtimeMetadataSpiArtifact",
            "runtimeMetadataOxiaArtifact",
            "sourceLocks",
            "executorManifest"
        };
        String[] exactDigests = {
            tuple.oxiaClientJarSha256().toHex(),
            tuple.testedEvidenceArtifactSha256().toHex(),
            tuple.runtimeDomainArtifactSha256().toHex(),
            tuple.runtimeMetadataSpiArtifactSha256().toHex(),
            tuple.runtimeMetadataOxiaArtifactSha256().toHex(),
            tuple.sourceLocksSha256().toHex(),
            tuple.executorManifestSha256().toHex()
        };
        json.append("},\"sourceArtifacts\":{");
        for (int index = 0; index < logicalNames.length; index++) {
            if (index > 0) {
                json.append(',');
            }
            Path artifact = sourceArtifactPaths.get(index);
            json.append('\"')
                    .append(logicalNames[index])
                    .append("\":{\"basename\":\"")
                    .append(artifact.getFileName())
                    .append("\",\"bytes\":")
                    .append(Files.size(artifact))
                    .append(",\"sha256\":\"")
                    .append(exactDigests[index])
                    .append("\"}");
        }
        json.append("}}\n");
        String zeroed = json.toString();
        String selfSha = sha256(zeroed.getBytes(StandardCharsets.UTF_8));
        int selfOffset = zeroed.indexOf(ZERO_SHA);
        String sealed = zeroed.substring(0, selfOffset)
                + selfSha
                + zeroed.substring(selfOffset + ZERO_SHA.length());
        Files.writeString(
                output,
                sealed,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
    }

    private static Path absolute(String value) {
        return Path.of(value).toAbsolutePath().normalize();
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
