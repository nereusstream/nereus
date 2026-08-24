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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/** Exact manifest/root-selected authority for releasing retained locator budget. */
public final class KafkaObjectTailRetirementAuthorityV1 {
    private final KafkaObjectBindingKeyV1 binding;
    private final Sha256Digest walRunRootSha;
    private final String manifestKey;
    private final Sha256Digest manifestSha;
    private final Sha256Digest selectedRootSha;
    private final List<KafkaObjectExtentLocatorV1> retiredLocators;

    private KafkaObjectTailRetirementAuthorityV1(
            KafkaObjectBindingKeyV1 binding,
            Sha256Digest walRunRootSha,
            String manifestKey,
            Sha256Digest manifestSha,
            Sha256Digest selectedRootSha,
            List<KafkaObjectExtentLocatorV1> retiredLocators) {
        this.binding = Objects.requireNonNull(binding, "binding");
        this.walRunRootSha = Objects.requireNonNull(walRunRootSha, "walRunRootSha");
        this.manifestKey = Objects.requireNonNull(manifestKey, "manifestKey");
        this.manifestSha = Objects.requireNonNull(manifestSha, "manifestSha");
        this.selectedRootSha = Objects.requireNonNull(selectedRootSha, "selectedRootSha");
        this.retiredLocators = List.copyOf(Objects.requireNonNull(retiredLocators, "retiredLocators"));
        int keyBytes = manifestKey.getBytes(StandardCharsets.UTF_8).length;
        if (walRunRootSha.isZero()
                || manifestSha.isZero()
                || selectedRootSha.isZero()
                || keyBytes == 0
                || keyBytes > 1024
                || manifestKey.indexOf('\0') >= 0
                || this.retiredLocators.isEmpty()) {
            throw new IllegalArgumentException("Kafka Object retirement authority is outside its exact domain");
        }
        this.retiredLocators.forEach(locator -> {
            if (!locator.binding().equals(binding)
                    || !locator.extent().walRunRootSha().equals(walRunRootSha)) {
                throw new IllegalArgumentException("retired locator differs from the manifest/root authority");
            }
        });
    }

    /** Issued only by the coherent-retirement adapter after its exact root CAS has selected the manifest cut. */
    static KafkaObjectTailRetirementAuthorityV1 afterRootCas(
            KafkaObjectBindingKeyV1 binding,
            Sha256Digest walRunRootSha,
            String manifestKey,
            Sha256Digest manifestSha,
            Sha256Digest selectedRootSha,
            List<KafkaObjectExtentLocatorV1> retiredLocators) {
        return new KafkaObjectTailRetirementAuthorityV1(
                binding, walRunRootSha, manifestKey, manifestSha, selectedRootSha, retiredLocators);
    }

    public List<KafkaObjectExtentLocatorV1> retiredLocators() {
        return retiredLocators;
    }
}
