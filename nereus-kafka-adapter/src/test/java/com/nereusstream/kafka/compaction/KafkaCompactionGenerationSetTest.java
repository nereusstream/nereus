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

package com.nereusstream.kafka.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.GenerationId;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.GenerationCommitResult;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaCompactionGenerationSetTest {
    private static final StreamId STREAM = new StreamId("stream-generation-set");

    @Test
    void canonicalDigestFreezesOrderedExactCommittedGenerationIdentities() {
        KafkaCompactionGenerationSet initial = KafkaCompactionGenerationSet.initial(generation(0, 10, 1, "a", 10));
        KafkaCompactionGenerationSet extended = initial.extend(generation(10, 20, 2, "b", 11));

        assertThat(extended.coverage()).isEqualTo(new OffsetRange(0, 20));
        assertThat(extended.generations()).hasSize(2);
        assertThat(extended.digestBytes()).hasSize(32);
        assertThat(initial.extend(generation(10, 20, 2, "b", 11)).digestSha256())
                .isEqualTo(extended.digestSha256());
        assertThat(initial.extend(generation(10, 20, 2, "c", 11)).digestSha256())
                .isNotEqualTo(extended.digestSha256());
    }

    @Test
    void rejectsGapsWrongViewsAndForgedDigests() {
        KafkaCompactionGenerationSet initial = KafkaCompactionGenerationSet.initial(generation(0, 10, 1, "a", 10));

        assertThatThrownBy(() -> initial.extend(generation(11, 20, 2, "b", 11)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("continue");
        assertThatThrownBy(() -> new KafkaCompactionGenerationSet(
                        STREAM,
                        initial.coverage(),
                        initial.generations(),
                        new Checksum(ChecksumType.SHA256, "f".repeat(64))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("digest");
        GenerationCommitResult committed = generation(0, 10, 1, "a", 10);
        assertThatThrownBy(() -> KafkaCompactionGenerationSet.initial(new GenerationCommitResult(
                        committed.streamId(),
                        ReadView.COMMITTED,
                        committed.coverage(),
                        committed.generation(),
                        committed.publicationId(),
                        committed.indexKey(),
                        committed.indexMetadataVersion(),
                        committed.indexRecordSha256(),
                        true)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TOPIC_COMPACTED");
    }

    @Test
    void constructorDefensivelyCopiesTheGenerationList() {
        GenerationCommitResult committed = generation(0, 10, 1, "a", 10);
        KafkaCompactionGenerationSet exact = KafkaCompactionGenerationSet.initial(committed);

        assertThat(new KafkaCompactionGenerationSet(
                        exact.streamId(), exact.coverage(), List.of(committed), exact.digestSha256()))
                .isEqualTo(exact);
    }

    private static GenerationCommitResult generation(
            long start, long end, long generation, String identitySeed, long version) {
        return new GenerationCommitResult(
                STREAM,
                ReadView.TOPIC_COMPACTED,
                new OffsetRange(start, end),
                new GenerationId(generation),
                new PublicationId(identitySeed.repeat(26)),
                "generation-index/" + identitySeed,
                version,
                new Checksum(ChecksumType.SHA256, identitySeed.repeat(64)),
                true);
    }
}
