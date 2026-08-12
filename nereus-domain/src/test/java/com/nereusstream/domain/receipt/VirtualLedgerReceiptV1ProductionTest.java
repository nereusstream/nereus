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

package com.nereusstream.domain.receipt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.AttachmentKind;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.AttachmentRef;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptKind;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptRejectedException;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptRoot;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.RejectionCode;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ScenarioResult;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.SourceTuple;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.SuiteResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VirtualLedgerReceiptV1ProductionTest {
    @Test
    void publicProductionCodecRoundTripsTheClosedCanonicalRoot() {
        ReceiptRoot root = receipt(List.of());
        byte[] canonical = VirtualLedgerReceiptV1.canonicalBytes(root);

        assertThat(VirtualLedgerReceiptV1.parseCanonical(canonical)).isEqualTo(root);
        assertThat(VirtualLedgerReceiptV1.sourceTupleSha256(root.sourceTuple())).hasSize(64);
    }

    @Test
    void nonCanonicalWhitespaceNeverBecomesAnEquivalentReceipt() {
        byte[] canonical = VirtualLedgerReceiptV1.canonicalBytes(receipt(List.of()));
        byte[] changed = (new String(canonical, StandardCharsets.UTF_8) + "\n").getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> VirtualLedgerReceiptV1.parseCanonical(changed))
                .isInstanceOf(ReceiptRejectedException.class)
                .extracting(error -> ((ReceiptRejectedException) error).code())
                .isEqualTo(RejectionCode.RECEIPT_NON_CANONICAL_JSON);
    }

    @Test
    void mandatoryPassIsDerivedFromLeafAccounting() {
        ReceiptRoot root = receipt(List.of());
        VirtualLedgerReceiptV1.requireMandatoryPass(root, Set.of("r1.registry"));

        ReceiptRoot failed = new ReceiptRoot(
                root.schema(),
                root.kind(),
                root.sourceTuple(),
                List.of(new ScenarioResult(
                        "V2-POSITION-003", List.of(new SuiteResult("r1.registry", 1, 1, 0, 1, 0, 0)))),
                List.of());
        assertThatThrownBy(() -> VirtualLedgerReceiptV1.requireMandatoryPass(failed, Set.of("r1.registry")))
                .isInstanceOf(ReceiptRejectedException.class)
                .extracting(error -> ((ReceiptRejectedException) error).code())
                .isEqualTo(RejectionCode.RECEIPT_MANDATORY_RESULT_NOT_PASS);
    }

    @Test
    void attachmentVerifierStreamsExactRegularBytes(@TempDir Path directory) throws Exception {
        Path attachment = directory.resolve("reports/r1.txt");
        Files.createDirectories(attachment.getParent());
        byte[] content = "r1-registry-conformance\n".getBytes(StandardCharsets.UTF_8);
        Files.write(attachment, content);
        ReceiptRoot root = receipt(List.of(new AttachmentRef(
                AttachmentKind.TEST_REPORT, "reports/r1.txt", content.length, VirtualLedgerReceiptV1.sha256(content))));

        assertThat(VirtualLedgerReceiptV1.verifyAttachments(directory, root))
                .containsEntry(AttachmentKind.TEST_REPORT, (long) content.length);
    }

    @Test
    void attachmentVerifierRejectsSymlinkSubstitution(@TempDir Path directory) throws Exception {
        byte[] content = "trusted\n".getBytes(StandardCharsets.UTF_8);
        Path target = directory.resolve("target.txt");
        Files.write(target, content);
        Files.createDirectories(directory.resolve("reports"));
        Files.createSymbolicLink(directory.resolve("reports/r1.txt"), target);
        ReceiptRoot root = receipt(List.of(new AttachmentRef(
                AttachmentKind.TEST_REPORT, "reports/r1.txt", content.length, VirtualLedgerReceiptV1.sha256(content))));

        assertThatThrownBy(() -> VirtualLedgerReceiptV1.verifyAttachments(directory, root))
                .isInstanceOf(ReceiptRejectedException.class)
                .extracting(error -> ((ReceiptRejectedException) error).code())
                .isEqualTo(RejectionCode.RECEIPT_ATTACHMENT_SYMLINK);
    }

    static ReceiptRoot receipt(List<AttachmentRef> attachments) {
        return new ReceiptRoot(
                VirtualLedgerReceiptV1.SCHEMA,
                ReceiptKind.REGISTRY_CONFORMANCE,
                sourceTuple('1'),
                List.of(new ScenarioResult(
                        "V2-POSITION-003", List.of(new SuiteResult("r1.registry", 2, 2, 2, 0, 0, 0)))),
                attachments);
    }

    static SourceTuple sourceTuple(char seed) {
        String commit = String.valueOf(seed).repeat(40);
        String digest = String.valueOf(seed).repeat(64);
        return new SourceTuple(
                commit, commit, commit, commit, commit, digest, digest, digest, digest, "sha256:" + digest, digest);
    }
}
