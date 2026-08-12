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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Set;

/** Minimal command-line entry point for trusted G1 receipt and Final validation. */
public final class M1EvidenceCli {
    private M1EvidenceCli() {}

    public static void main(String[] arguments) {
        if (arguments.length == 0) {
            throw new IllegalArgumentException("missing command");
        }
        switch (arguments[0]) {
            case "validate-receipt" -> {
                requireArguments(arguments, 2);
                validateReceipt(Path.of(arguments[1]));
            }
            case "validate-final" -> {
                requireArguments(arguments, 2);
                validateFinal(Path.of(arguments[1]));
            }
            case "write-gate-result" -> {
                requireArguments(arguments, 5);
                writeGateResult(arguments[1], arguments[2], arguments[3], Path.of(arguments[4]));
            }
            default -> throw new IllegalArgumentException("unknown command: " + arguments[0]);
        }
    }

    private static void requireArguments(String[] arguments, int count) {
        if (arguments.length != count) {
            throw new IllegalArgumentException("wrong argument count for " + arguments[0]);
        }
    }

    private static void validateReceipt(Path file) {
        VirtualLedgerReceiptV1.ReceiptRoot receipt = VirtualLedgerReceiptV1.parseCanonicalFile(file);
        VirtualLedgerReceiptV1.requireMandatoryPass(receipt, Set.of());
        VirtualLedgerReceiptV1.verifyAttachments(file.toAbsolutePath().getParent(), receipt);
        System.out.printf(
                "VALID receipt kind=%s scenarios=%d sourceTupleSha=%s%n",
                receipt.kind(),
                receipt.scenarios().size(),
                VirtualLedgerReceiptV1.sourceTupleSha256(receipt.sourceTuple()));
    }

    private static void validateFinal(Path file) {
        M1FinalResolverV1.Resolution resolution = M1FinalResolverV1.resolve(file, M1PromotionPolicyV1.policy());
        System.out.printf(
                "PASS M1_FINAL scenarios=%d gates=%d receipts=%d sourceTupleSha=%s%n",
                resolution.passedScenarios().size(),
                resolution.passedGates().size(),
                resolution.receiptPaths().size(),
                resolution.sourceTupleSha());
    }

    private static void writeGateResult(String gateId, String sourceTupleSha, String outcome, Path output) {
        M1FinalIndexV1.GateResult result = new M1FinalIndexV1.GateResult(
                M1FinalIndexV1.GATE_RESULT_SCHEMA,
                M1FinalIndexV1.GateId.valueOf(gateId),
                M1FinalIndexV1.GateOutcome.valueOf(outcome),
                sourceTupleSha);
        byte[] canonical = M1FinalIndexV1.canonicalBytes(result);
        try {
            Path parent = output.toAbsolutePath().getParent();
            if (parent == null) {
                throw new IllegalArgumentException("gate-result output has no parent");
            }
            Files.createDirectories(parent);
            try {
                Files.write(output, canonical, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            } catch (java.nio.file.FileAlreadyExistsException exists) {
                byte[] current = Files.readAllBytes(output);
                if (!Arrays.equals(current, canonical)) {
                    throw new IllegalStateException("existing gate result differs from canonical bytes", exists);
                }
            }
        } catch (IOException error) {
            throw new IllegalStateException("cannot write canonical gate result", error);
        }
        System.out.printf("WROTE gate=%s outcome=%s bytes=%d path=%s%n", gateId, outcome, canonical.length, output);
    }
}
