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

import java.nio.file.Path;

/** Minimal production entry point for validating an already-produced canonical Kafka Inputs receipt. */
public final class KafkaM2InputsReceiptCli {
    private KafkaM2InputsReceiptCli() {}

    public static void main(String[] arguments) {
        if (arguments.length != 2 || !"validate".equals(arguments[0])) {
            throw new IllegalArgumentException("usage: validate <canonical-kafka-inputs-receipt>");
        }
        KafkaM2InputsReceiptV1.Receipt receipt = KafkaM2InputsReceiptV1.parseCanonicalFile(Path.of(arguments[1]));
        long suites = receipt.childGates().stream()
                .mapToLong(KafkaM2InputsReceiptV1.GateResult::suites)
                .sum();
        long tests = receipt.childGates().stream()
                .mapToLong(KafkaM2InputsReceiptV1.GateResult::tests)
                .sum();
        System.out.println("Kafka M2 Inputs receipt PASS: gates="
                + receipt.childGates().size() + " suites=" + suites + " tests=" + tests + " promotionEligible=false");
    }
}
