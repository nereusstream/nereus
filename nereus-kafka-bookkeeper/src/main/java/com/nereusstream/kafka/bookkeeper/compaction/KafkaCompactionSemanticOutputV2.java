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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.DispositionRow;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Gap;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ParsedBatch;
import java.util.List;
import java.util.Objects;

/**
 * Compiled Kafka semantics before a carrier allocates storage.
 *
 * <p>Batch ordinals and byte offsets describe only the canonical concatenation. A carrier must bind each ordinal to
 * exact persisted bytes and validate its own physical locators; these indexes alone authorize no read publication.
 */
public record KafkaCompactionSemanticOutputV2(
        Sha256Digest taskIdSha256,
        Sha256Digest outputIdentitySha256,
        Sha256Digest compactionPlanRootSha256,
        Sha256Digest compactionTaskIdSha256,
        List<ParsedBatch> inputBatches,
        List<KafkaSemanticCompactorV1.BatchOutput> batchOutputs,
        List<DispositionRow> dispositions,
        List<Gap> gaps,
        List<KafkaCompactionIndexV1> indexes,
        List<CanonicalBytes> indexBodies)
        implements KafkaCompactionSemanticViewV2 {
    public KafkaCompactionSemanticOutputV2 {
        KafkaCompactionRecordsV1.requireDigest(taskIdSha256, "taskIdSha256");
        KafkaCompactionRecordsV1.requireDigest(outputIdentitySha256, "outputIdentitySha256");
        KafkaCompactionRecordsV1.requireDigest(compactionPlanRootSha256, "compactionPlanRootSha256");
        KafkaCompactionRecordsV1.requireDigest(compactionTaskIdSha256, "compactionTaskIdSha256");
        inputBatches = List.copyOf(Objects.requireNonNull(inputBatches, "inputBatches"));
        batchOutputs = List.copyOf(Objects.requireNonNull(batchOutputs, "batchOutputs"));
        dispositions = List.copyOf(Objects.requireNonNull(dispositions, "dispositions"));
        gaps = List.copyOf(Objects.requireNonNull(gaps, "gaps"));
        indexes = List.copyOf(Objects.requireNonNull(indexes, "indexes"));
        indexBodies = List.copyOf(Objects.requireNonNull(indexBodies, "indexBodies"));
    }
}
