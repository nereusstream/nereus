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

/** Carrier-independent input to the shared record and eight-index semantic validator. */
public interface KafkaCompactionSemanticViewV2 {
    Sha256Digest taskIdSha256();

    Sha256Digest outputIdentitySha256();

    List<ParsedBatch> inputBatches();

    List<KafkaSemanticCompactorV1.BatchOutput> batchOutputs();

    List<DispositionRow> dispositions();

    List<Gap> gaps();

    List<KafkaCompactionIndexV1> indexes();

    List<CanonicalBytes> indexBodies();

    default List<ParsedBatch> outputBatches() {
        return batchOutputs().stream().flatMap(value -> value.output().stream()).toList();
    }
}
