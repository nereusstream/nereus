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

package com.nereusstream.kafka.bookkeeper.nbke2;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** First entry of one NBKE2 run. */
public record Nbke2RunHeaderV1(
        Nbke2RunBindingV1 runBinding,
        long kafkaStartOffset,
        long firstDataEntryId,
        Sha256Digest ledgerConfigurationDigest)
        implements Nbke2FrameV1 {
    public Nbke2RunHeaderV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(ledgerConfigurationDigest, "ledgerConfigurationDigest");
        if (kafkaStartOffset < 0 || firstDataEntryId < 0 || ledgerConfigurationDigest.isZero()) {
            throw new IllegalArgumentException("run-header offsets/configuration are outside the v1 domain");
        }
    }

    @Override
    public Nbke2FrameTypeV1 frameType() {
        return Nbke2FrameTypeV1.RUN_HEADER;
    }
}
