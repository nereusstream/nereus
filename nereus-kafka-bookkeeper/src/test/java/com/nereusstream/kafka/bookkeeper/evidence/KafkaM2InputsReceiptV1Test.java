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

package com.nereusstream.kafka.bookkeeper.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2InputsReceiptV1.GateId;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2InputsReceiptV1.GateResult;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2InputsReceiptV1.Receipt;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2InputsReceiptV1.ReceiptKind;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2InputsReceiptV1.ReceiptRejectedException;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2InputsReceiptV1.ReceiptResult;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2InputsReceiptV1.RejectionCode;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2InputsReceiptV1.SourceTuple;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaM2InputsReceiptV1Test {
    @TempDir
    Path temporaryDirectory;

    @Test
    void canonicalRoundTripPreservesTheExactNonPromotableReceipt() {
        Receipt receipt = validReceipt();
        byte[] canonical = KafkaM2InputsReceiptV1.canonicalBytes(receipt);

        assertThat(KafkaM2InputsReceiptV1.parseCanonical(canonical)).isEqualTo(receipt);
        assertThat(new String(canonical, StandardCharsets.UTF_8))
                .startsWith("{\"childGates\":[{\"errors\":0")
                .contains("\"promotionEligible\":false")
                .endsWith("}");
    }

    @Test
    void rejectsWhitespaceTrailingBytesUnknownOrderAndMalformedUtf8() {
        byte[] canonical = KafkaM2InputsReceiptV1.canonicalBytes(validReceipt());

        assertRejected((" " + new String(canonical, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8));
        assertRejected(Arrays.copyOf(canonical, canonical.length + 1));
        String reordered = new String(canonical, StandardCharsets.UTF_8)
                .replace("{\"childGates\":", "{\"kind\":\"KAFKA_M2_INPUTS_ONLY\",\"childGates\":");
        assertRejected(reordered.getBytes(StandardCharsets.UTF_8));
        assertRejected(new byte[] {(byte) 0xc3, (byte) 0x28});
    }

    @Test
    void rejectsPromotionAndUnknownSchemaKindOrResult() {
        Receipt valid = validReceipt();
        Receipt promotable = new Receipt(
                valid.schema(), valid.kind(), true, valid.result(), valid.sourceTuple(), valid.childGates());
        assertThatThrownBy(() -> KafkaM2InputsReceiptV1.canonicalBytes(promotable))
                .isInstanceOfSatisfying(ReceiptRejectedException.class, rejection -> assertThat(rejection.code())
                        .isEqualTo(RejectionCode.PROMOTION_FORBIDDEN));

        Receipt wrongSchema =
                new Receipt("UNKNOWN", valid.kind(), false, valid.result(), valid.sourceTuple(), valid.childGates());
        assertThatThrownBy(() -> KafkaM2InputsReceiptV1.canonicalBytes(wrongSchema))
                .isInstanceOfSatisfying(ReceiptRejectedException.class, rejection -> assertThat(rejection.code())
                        .isEqualTo(RejectionCode.SCHEMA_KIND_RESULT_INVALID));
    }

    @Test
    void requiresFiveSortedNonZeroZeroSkipPassingChildGates() {
        Receipt valid = validReceipt();
        List<GateResult> missing = new ArrayList<>(valid.childGates());
        missing.remove(missing.size() - 1);
        assertGateRejected(valid, missing, RejectionCode.GATE_SET_INVALID);

        List<GateResult> reordered = new ArrayList<>(valid.childGates());
        GateResult first = reordered.remove(0);
        reordered.add(first);
        assertGateRejected(valid, reordered, RejectionCode.GATE_SET_INVALID);

        List<GateResult> failed = new ArrayList<>(valid.childGates());
        failed.set(0, new GateResult(GateId.K0_E, 1, 1, 1, 0, 0));
        assertGateRejected(valid, failed, RejectionCode.GATE_RESULT_NOT_PASS);

        List<GateResult> empty = new ArrayList<>(valid.childGates());
        empty.set(0, new GateResult(GateId.K0_E, 1, 0, 0, 0, 0));
        assertGateRejected(valid, empty, RejectionCode.GATE_RESULT_NOT_PASS);
    }

    @Test
    void rejectsMalformedCommitShaAndImageDigestSourceFields() {
        Receipt valid = validReceipt();
        SourceTuple source = valid.sourceTuple();
        SourceTuple invalid = new SourceTuple(
                "x",
                source.bookKeeperClientJarSha256(),
                source.bookKeeperClientPomSha256(),
                source.bookKeeperImageConfigDigest(),
                source.bookKeeperImageManifestDigest(),
                source.bookKeeperSourceCommit(),
                source.bookKeeperTagObject(),
                source.k0ModuleManifestSha256(),
                source.k0ModuleReceiptSha256(),
                source.kafkaBaseCommit(),
                source.kafkaForkCommit(),
                source.m1FinalIndexSha256(),
                source.m1SourceTupleSha256(),
                source.n1ManifestSha256(),
                source.n1SourceCommit(),
                source.nbke2GoldensSha256(),
                source.nbke2ProjectionSha256(),
                source.nereusCommit(),
                source.numericProjectionSha256(),
                source.sourceLocksSha256());
        Receipt invalidReceipt =
                new Receipt(valid.schema(), valid.kind(), false, valid.result(), invalid, valid.childGates());

        assertThatThrownBy(() -> KafkaM2InputsReceiptV1.canonicalBytes(invalidReceipt))
                .isInstanceOfSatisfying(ReceiptRejectedException.class, rejection -> assertThat(rejection.code())
                        .isEqualTo(RejectionCode.SOURCE_TUPLE_INVALID));
    }

    @Test
    void canonicalFileParserRejectsSymlinkAndOversizedRoot() throws Exception {
        byte[] canonical = KafkaM2InputsReceiptV1.canonicalBytes(validReceipt());
        Path regular = temporaryDirectory.resolve("receipt.json");
        Files.write(regular, canonical);
        assertThat(KafkaM2InputsReceiptV1.parseCanonicalFile(regular)).isEqualTo(validReceipt());

        Path symlink = temporaryDirectory.resolve("receipt-link.json");
        Files.createSymbolicLink(symlink, regular.getFileName());
        assertThatThrownBy(() -> KafkaM2InputsReceiptV1.parseCanonicalFile(symlink))
                .isInstanceOfSatisfying(ReceiptRejectedException.class, rejection -> assertThat(rejection.code())
                        .isEqualTo(RejectionCode.ROOT_NOT_REGULAR));

        Path oversized = temporaryDirectory.resolve("oversized.json");
        Files.write(oversized, new byte[KafkaM2InputsReceiptV1.MAX_CANONICAL_BYTES + 1]);
        assertThatThrownBy(() -> KafkaM2InputsReceiptV1.parseCanonicalFile(oversized))
                .isInstanceOfSatisfying(ReceiptRejectedException.class, rejection -> assertThat(rejection.code())
                        .isEqualTo(RejectionCode.ROOT_BYTES_EXCEEDED));
    }

    private static Receipt validReceipt() {
        String sha = "a".repeat(64);
        String commit = "b".repeat(40);
        SourceTuple source = new SourceTuple(
                sha,
                sha,
                sha,
                "sha256:" + sha,
                "sha256:" + sha,
                commit,
                commit,
                sha,
                sha,
                commit,
                commit,
                sha,
                sha,
                sha,
                commit,
                sha,
                sha,
                commit,
                sha,
                sha);
        List<GateResult> gates = Arrays.stream(GateId.values())
                .map(gate -> new GateResult(gate, 1, 1, 0, 0, 0))
                .toList();
        return new Receipt(
                KafkaM2InputsReceiptV1.SCHEMA,
                ReceiptKind.KAFKA_M2_INPUTS_ONLY,
                false,
                ReceiptResult.PASS_KAFKA_M2_INPUTS_ONLY,
                source,
                gates);
    }

    private static void assertRejected(byte[] bytes) {
        assertThatThrownBy(() -> KafkaM2InputsReceiptV1.parseCanonical(bytes))
                .isInstanceOf(ReceiptRejectedException.class);
    }

    private static void assertGateRejected(Receipt valid, List<GateResult> gates, RejectionCode expectedRejection) {
        Receipt changed = new Receipt(valid.schema(), valid.kind(), false, valid.result(), valid.sourceTuple(), gates);
        assertThatThrownBy(() -> KafkaM2InputsReceiptV1.canonicalBytes(changed))
                .isInstanceOfSatisfying(ReceiptRejectedException.class, rejection -> assertThat(rejection.code())
                        .isEqualTo(expectedRejection));
    }
}
