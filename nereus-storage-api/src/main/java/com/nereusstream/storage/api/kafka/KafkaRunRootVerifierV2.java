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

package com.nereusstream.storage.api.kafka;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.concurrent.CompletionStage;

/** Actual native header/seal verification. This does not replace protocol-owner admission or physical eligibility. */
public interface KafkaRunRootVerifierV2 {
    Sha256Digest capabilitySha256();

    CompletionStage<Void> requireNative(KafkaRunRootRecordV2 root);

    /** Independently verifies the fixed native closed ledger and complete continuous prefix in a crash cut. */
    default CompletionStage<Void> requireRecovered(KafkaRunRootRecordV2 root, KafkaOwnerAdmissionV1 closedOwner) {
        return java.util.concurrent.CompletableFuture.failedFuture(
                new UnsupportedOperationException("native crash-run prefix verification is unavailable"));
    }
}
