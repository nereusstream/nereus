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

import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationObjectSessionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.MaterializationDataReader;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.ValidatedGeneration;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Reuses M5-A exact Object creation and selector-publication inputs after a fresh M5-B semantic validation. */
public final class KafkaM5CompactionBridgeV1 {
    public record CreatedGeneration(List<GenerationObject> payloadObjects, List<GenerationObject> indexObjects) {
        public CreatedGeneration {
            payloadObjects = List.copyOf(Objects.requireNonNull(payloadObjects, "payloadObjects"));
            indexObjects = List.copyOf(Objects.requireNonNull(indexObjects, "indexObjects"));
            if (payloadObjects.size() != 1 || indexObjects.size() != 8) {
                throw new IllegalArgumentException(
                        "M5-B created generation does not contain one payload and 8 indexes");
            }
        }
    }

    public CreatedGeneration createExact(
            KafkaSemanticCompactorV1.Result result, M5MaterializationObjectSessionV1 objectSession) throws IOException {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(objectSession, "objectSession");
        var payload = objectSession.createExact(List.of(result.candidate().payloadCandidate()));
        var indexes = objectSession.createExact(result.candidate().indexCandidates());
        return new CreatedGeneration(payload.exactObjects(), indexes.exactObjects());
    }

    public ValidatedGeneration validateForPublication(
            CompactionPlan plan,
            KafkaSemanticCompactorV1.Result result,
            CreatedGeneration created,
            BindingReadSelector currentSelector,
            List<SourceProtectionIdentity> exactFallbackSources,
            MaterializationDataReader reader,
            KafkaCompactionPublicationFenceV1.CurrentStateReader currentCompactionState)
            throws IOException {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(created, "created");
        var rereadProof = new KafkaCompactionSemanticValidatorV1().validate(plan, result.candidate());
        if (!rereadProof.equals(result.semanticProof())) {
            throw new IllegalStateException("M5-B semantic proof changed between build and publication");
        }
        new KafkaCompactionPublicationFenceV1().requireCurrent(plan, currentCompactionState);
        return new M5MaterializationValidatorV1()
                .validateSemantic(
                        result.candidate().materializationPlan(),
                        currentSelector,
                        exactFallbackSources,
                        created.payloadObjects(),
                        created.indexObjects(),
                        reader,
                        rereadProof);
    }
}
