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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import java.util.Objects;

/** Exact physical Root plus Kafka ProviderScope/StorageRun context for NWKCP1 selection. */
public record KafkaNwkcp1WalRunContextV1(Sha256Digest rootSha, Nbke2RunBindingV1 kafkaRunBinding) {
    public KafkaNwkcp1WalRunContextV1 {
        Objects.requireNonNull(rootSha, "rootSha");
        Objects.requireNonNull(kafkaRunBinding, "kafkaRunBinding");
        if (rootSha.isZero()) {
            throw new IllegalArgumentException("NWKCP1 WalRun Root SHA is zero");
        }
    }
}
