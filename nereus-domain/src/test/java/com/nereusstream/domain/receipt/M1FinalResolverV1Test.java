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
import com.nereusstream.domain.receipt.M1FinalIndexV1.FinalRejectedException;
import com.nereusstream.domain.receipt.M1FinalIndexV1.GateId;
import com.nereusstream.domain.receipt.M1FinalIndexV1.GateOutcome;
import com.nereusstream.domain.receipt.M1FinalIndexV1.GateRef;
import com.nereusstream.domain.receipt.M1FinalIndexV1.GateResult;
import com.nereusstream.domain.receipt.M1FinalIndexV1.Index;
import com.nereusstream.domain.receipt.M1FinalIndexV1.ReceiptRef;
import com.nereusstream.domain.receipt.M1FinalIndexV1.RejectionCode;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptKind;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptRoot;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M1FinalResolverV1Test {
    @Test
    void canonicalIndexAndGateResultRoundTrip() {
        String tupleSha = "1".repeat(64);
        GateResult gate =
                new GateResult(M1FinalIndexV1.GATE_RESULT_SCHEMA, GateId.V2_M1_FAST, GateOutcome.PASS, tupleSha);
        Index index = new Index(
                M1FinalIndexV1.INDEX_SCHEMA,
                tupleSha,
                List.of(new GateRef(GateId.V2_M1_FAST, "gates/fast.json", 1, "2".repeat(64))),
                List.of(new ReceiptRef(ReceiptKind.REGISTRY_CONFORMANCE, "receipts/r1.json", 1, "3".repeat(64))));

        assertThat(M1FinalIndexV1.parseCanonicalGateResult(M1FinalIndexV1.canonicalBytes(gate)))
                .isEqualTo(gate);
        assertThat(M1FinalIndexV1.parseCanonical(M1FinalIndexV1.canonicalBytes(index)))
                .isEqualTo(index);
    }

    @Test
    void resolverAggregatesAlreadyExecutedEvidenceWithoutRerunningIt(@TempDir Path directory) throws Exception {
        Fixture fixture = fixture(directory, GateOutcome.PASS, true);

        M1FinalResolverV1.Resolution result = M1FinalResolverV1.resolve(fixture.index(), policy());

        assertThat(result.sourceTuple()).isEqualTo(fixture.receipt().sourceTuple());
        assertThat(result.passedGates()).containsExactlyInAnyOrder(GateId.V2_M1_FAST, GateId.V2_M1_EXACT_SOURCE);
        assertThat(result.passedScenarios()).containsExactly("V2-POSITION-003");
        assertThat(result.receiptPaths()).containsExactly("receipts/registry.json");
    }

    @Test
    void missingExactSourceGateFailsClosed(@TempDir Path directory) throws Exception {
        Fixture fixture = fixture(directory, GateOutcome.PASS, false);

        assertThatThrownBy(() -> M1FinalResolverV1.resolve(fixture.index(), policy()))
                .isInstanceOf(FinalRejectedException.class)
                .extracting(error -> ((FinalRejectedException) error).code())
                .isEqualTo(RejectionCode.FINAL_REQUIRED_GATE_MISSING);
    }

    @Test
    void referencedFailedGateCannotBeOverriddenByTheIndex(@TempDir Path directory) throws Exception {
        Fixture fixture = fixture(directory, GateOutcome.FAIL, true);

        assertThatThrownBy(() -> M1FinalResolverV1.resolve(fixture.index(), policy()))
                .isInstanceOf(FinalRejectedException.class)
                .extracting(error -> ((FinalRejectedException) error).code())
                .isEqualTo(RejectionCode.FINAL_GATE_NOT_PASS);
    }

    @Test
    void receiptDigestMismatchFailsBeforeSemanticPromotion(@TempDir Path directory) throws Exception {
        Fixture fixture = fixture(directory, GateOutcome.PASS, true);
        Files.writeString(directory.resolve("receipts/registry.json"), "{}", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> M1FinalResolverV1.resolve(fixture.index(), policy()))
                .isInstanceOf(FinalRejectedException.class)
                .extracting(error -> ((FinalRejectedException) error).code())
                .isEqualTo(RejectionCode.FINAL_REFERENCE_LENGTH_MISMATCH);
    }

    @Test
    void nonCanonicalFinalIndexIsRejected(@TempDir Path directory) throws Exception {
        Fixture fixture = fixture(directory, GateOutcome.PASS, true);
        Files.writeString(fixture.index(), Files.readString(fixture.index()) + "\n", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> M1FinalResolverV1.resolve(fixture.index(), policy()))
                .isInstanceOf(FinalRejectedException.class)
                .extracting(error -> ((FinalRejectedException) error).code())
                .isEqualTo(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL);
    }

    private static M1FinalResolverV1.PromotionPolicy policy() {
        return new M1FinalResolverV1.PromotionPolicy(List.of(new M1FinalResolverV1.ScenarioRequirement(
                "V2-POSITION-003", ReceiptKind.REGISTRY_CONFORMANCE, Set.of("r1.registry"))));
    }

    private static Fixture fixture(Path directory, GateOutcome exactOutcome, boolean includeExact) throws Exception {
        Files.createDirectories(directory.resolve("gates"));
        Files.createDirectories(directory.resolve("receipts"));
        ReceiptRoot receipt = VirtualLedgerReceiptV1ProductionTest.receipt(List.of());
        byte[] receiptBytes = VirtualLedgerReceiptV1.canonicalBytes(receipt);
        Path receiptPath = directory.resolve("receipts/registry.json");
        Files.write(receiptPath, receiptBytes);
        String tupleSha = VirtualLedgerReceiptV1.sourceTupleSha256(receipt.sourceTuple());

        GateResult exact =
                new GateResult(M1FinalIndexV1.GATE_RESULT_SCHEMA, GateId.V2_M1_EXACT_SOURCE, exactOutcome, tupleSha);
        GateResult fast =
                new GateResult(M1FinalIndexV1.GATE_RESULT_SCHEMA, GateId.V2_M1_FAST, GateOutcome.PASS, tupleSha);
        byte[] exactBytes = M1FinalIndexV1.canonicalBytes(exact);
        byte[] fastBytes = M1FinalIndexV1.canonicalBytes(fast);
        Files.write(directory.resolve("gates/exact.json"), exactBytes);
        Files.write(directory.resolve("gates/fast.json"), fastBytes);

        List<GateRef> gates = includeExact
                ? List.of(
                        ref(GateId.V2_M1_EXACT_SOURCE, "gates/exact.json", exactBytes),
                        ref(GateId.V2_M1_FAST, "gates/fast.json", fastBytes))
                : List.of(ref(GateId.V2_M1_FAST, "gates/fast.json", fastBytes));
        Index index = new Index(
                M1FinalIndexV1.INDEX_SCHEMA,
                tupleSha,
                gates,
                List.of(new ReceiptRef(
                        ReceiptKind.REGISTRY_CONFORMANCE,
                        "receipts/registry.json",
                        receiptBytes.length,
                        VirtualLedgerReceiptV1.sha256(receiptBytes))));
        Path indexPath = directory.resolve("final-index.json");
        Files.write(indexPath, M1FinalIndexV1.canonicalBytes(index));
        return new Fixture(indexPath, receipt);
    }

    private static GateRef ref(GateId gateId, String path, byte[] bytes) {
        return new GateRef(gateId, path, bytes.length, VirtualLedgerReceiptV1.sha256(bytes));
    }

    private record Fixture(Path index, ReceiptRoot receipt) {}
}
