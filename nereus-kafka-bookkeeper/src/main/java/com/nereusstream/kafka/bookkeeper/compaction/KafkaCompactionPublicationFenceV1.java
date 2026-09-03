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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Frontiers;
import java.util.Objects;

/** Final policy/root/frontier reread required immediately before M5-A selector publication. */
public final class KafkaCompactionPublicationFenceV1 {
    @FunctionalInterface
    public interface CurrentStateReader {
        Snapshot readCurrent();
    }

    public record Snapshot(
            Sha256Digest compactionPlanRootSha256,
            Sha256Digest protocolStateRootSha256,
            long policyGeneration,
            Frontiers frontiers) {
        public Snapshot {
            KafkaCompactionRecordsV1.requireDigest(compactionPlanRootSha256, "compactionPlanRootSha256");
            KafkaCompactionRecordsV1.requireDigest(protocolStateRootSha256, "protocolStateRootSha256");
            Objects.requireNonNull(frontiers, "frontiers");
            if (policyGeneration <= 0) {
                throw new IllegalArgumentException("M5-B current policy generation is invalid");
            }
        }
    }

    public Snapshot expected(CompactionPlan plan) {
        Objects.requireNonNull(plan, "plan");
        return new Snapshot(
                KafkaCompactionCanonicalV1.planRoot(plan),
                KafkaCompactionCanonicalV1.protocolStateRoot(plan),
                plan.policy().policyGeneration(),
                plan.frontiers());
    }

    public void requireCurrent(CompactionPlan plan, CurrentStateReader reader) {
        Objects.requireNonNull(reader, "reader");
        if (!expected(plan).equals(reader.readCurrent())) {
            throw new IllegalStateException("M5-B policy/root/frontier fence is stale before publication");
        }
    }
}
