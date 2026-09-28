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

/** Current native Controller assignment, projected without inventing another assignment authority. */
public record KafkaOwnerIdentityV1(
        long ownerEpoch, int kafkaLeaderEpoch, int brokerId, long brokerEpoch, long metadataOffset) {
    public KafkaOwnerIdentityV1 {
        if (ownerEpoch <= 0 || kafkaLeaderEpoch < 0 || brokerId < 0 || brokerEpoch < 0 || metadataOffset < 0) {
            throw new IllegalArgumentException("owner identity is outside its native domains");
        }
    }

    public boolean succeeds(KafkaOwnerIdentityV1 previous) {
        return ownerEpoch > previous.ownerEpoch()
                && kafkaLeaderEpoch > previous.kafkaLeaderEpoch()
                && metadataOffset > previous.metadataOffset();
    }
}
