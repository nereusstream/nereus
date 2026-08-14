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

/** Production entry point for resolving an already-published canonical Kafka M2 Final receipt. */
public final class KafkaM2FinalReceiptCli {
    private KafkaM2FinalReceiptCli() {}

    public static void main(String[] arguments) {
        if (arguments.length != 3 || !"validate".equals(arguments[0])) {
            throw new IllegalArgumentException("usage: validate <repository-root> <canonical-kafka-final-receipt>");
        }
        KafkaM2FinalResolverV1.Resolution resolution =
                KafkaM2FinalResolverV1.resolve(Path.of(arguments[1]), Path.of(arguments[2]));
        System.out.println("Kafka M2 Final receipt PASS: source="
                + resolution.sourceTuple().nereusCommit()
                + " scenarios="
                + resolution.promotedScenarios().size()
                + " suiteReferences="
                + resolution.scenarioSuiteReferences()
                + " attachments="
                + resolution.uniqueAttachments());
    }
}
