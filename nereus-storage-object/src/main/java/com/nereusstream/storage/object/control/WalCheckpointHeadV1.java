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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;
import java.util.Optional;

/** One publisher-epoch-fenced physical checkpoint head for an open WalRun. */
public record WalCheckpointHeadV1(
        Sha256Digest rootSha256,
        long shardRunEpoch,
        long publisherEpoch,
        long pageOrdinal,
        Optional<String> pageKey,
        Optional<Sha256Digest> pageSha256,
        LaneSequenceVector coveredThrough) {
    public WalCheckpointHeadV1 {
        Objects.requireNonNull(rootSha256, "rootSha256");
        Objects.requireNonNull(pageKey, "pageKey");
        Objects.requireNonNull(pageSha256, "pageSha256");
        Objects.requireNonNull(coveredThrough, "coveredThrough");
        if (rootSha256.isZero() || shardRunEpoch < 0 || publisherEpoch < 0 || pageOrdinal < -1) {
            throw new IllegalArgumentException("checkpoint head identity is invalid");
        }
        if ((pageOrdinal == -1) != pageKey.isEmpty() || pageKey.isEmpty() != pageSha256.isEmpty()) {
            throw new IllegalArgumentException("empty checkpoint head must omit page identity and use ordinal -1");
        }
        if (pageOrdinal == -1 && !coveredThrough.equals(LaneSequenceVector.empty())) {
            throw new IllegalArgumentException("empty checkpoint head must carry the all-minus-one lane vector");
        }
        pageKey.ifPresent(value -> {
            if (value.isEmpty() || value.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("checkpoint page key is invalid");
            }
        });
    }

    public static WalCheckpointHeadV1 empty(Sha256Digest rootSha256, long shardRunEpoch, long publisherEpoch) {
        return new WalCheckpointHeadV1(
                rootSha256,
                shardRunEpoch,
                publisherEpoch,
                -1,
                Optional.empty(),
                Optional.empty(),
                LaneSequenceVector.empty());
    }
}
