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

package com.nereusstream.metadata.spi.capability;

import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Owns exact create/read/CAS operations for one Pulsar name-generation selector. */
public interface PulsarTopicGenerationSelectorStore {
    CompletionStage<Optional<VersionedSelectorSnapshot>> readSelector(PulsarPersistenceName persistenceName);

    CompletionStage<CreateMutationResult<VersionedSelectorSnapshot>> createSelector(
            PulsarTopicGenerationSelectorValueV1 candidate);

    CompletionStage<ConditionalCasResult<VersionedSelectorSnapshot>> compareAndSetSelector(
            VersionedSelectorSnapshot exactPredecessor, PulsarTopicGenerationSelectorValueV1 candidate);
}
