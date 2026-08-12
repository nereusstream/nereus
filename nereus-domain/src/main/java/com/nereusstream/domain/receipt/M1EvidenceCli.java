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

import java.nio.file.Path;
import java.util.Set;

/** Minimal command-line entry point for trusted G1 receipt and Final validation. */
public final class M1EvidenceCli {
    private M1EvidenceCli() {}

    public static void main(String[] arguments) {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: validate-receipt|validate-final <canonical-file>");
        }
        Path file = Path.of(arguments[1]);
        switch (arguments[0]) {
            case "validate-receipt" -> validateReceipt(file);
            case "validate-final" -> validateFinal(file);
            default -> throw new IllegalArgumentException("unknown command: " + arguments[0]);
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
}
