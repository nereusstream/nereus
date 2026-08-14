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

import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Low-frequency create/open/seal/successor authority for Kafka run roots.
 *
 * <p>This is deliberately not generic metadata CRUD and is never invoked once per Produce, RecordBatch, or Fetch.
 */
public interface KafkaRunRootAuthority {
    CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> createRoot(
            KafkaRunRootSnapshotV1 activeCandidate);

    CompletionStage<Optional<KafkaRunRootSnapshotV1>> openRoot(StorageRunId runId);

    CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> sealRoot(
            KafkaRunRootSnapshotV1 expectedActive, KafkaRunRootSnapshotV1 sealedCandidate);

    CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> createSuccessor(
            KafkaRunRootSnapshotV1 expectedSealed, KafkaRunRootSnapshotV1 activeSuccessor);
}
