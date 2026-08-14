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

package com.nereusstream.kafka.bookkeeper.admission;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperNumericProjectionV1Test {
    private static final Path PROJECTION = Path.of("..", "docs", "v2", "wire", "kafka-m2-k0-numeric-v1.json");

    @Test
    void independentNumericProjectionMatchesPersistedProductionCapsAndAdmissionBoundary() throws Exception {
        String projection = Files.readString(PROJECTION);

        assertThat(projection)
                .contains("\"schema\": \"NEREUS_V2_M2_KAFKA_K0_NUMERIC_V1\"")
                .contains("\"formatMaxFrameBytes\": " + Nbke2ConstantsV1.FORMAT_MAX_FRAME_BYTES)
                .contains("\"formatMaxDataPayloadBytes\": " + Nbke2ConstantsV1.FORMAT_MAX_DATA_PAYLOAD_BYTES)
                .contains("\"formatMaxTopicNameBytes\": " + Nbke2ConstantsV1.FORMAT_MAX_TOPIC_NAME_BYTES)
                .contains("\"formatMaxLocatorCount\": " + Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT)
                .contains("\"formatMaxIndexDirectoryCount\": " + Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT)
                .contains(
                        "\"formatMaxCheckpointSectionBytes\": " + Nbke2ConstantsV1.FORMAT_MAX_CHECKPOINT_SECTION_BYTES)
                .contains("BEFORE_OFFSET_OR_ENTRY_ALLOCATION")
                .contains("KafkaBookKeeperDataAdmissionTicketV1")
                .contains("ENTRY_COUNT")
                .contains("ENCODED_BYTES")
                .contains("ELAPSED_NANOS")
                .contains("NON_NEGATIVE_CHECKED_ADD");
    }
}
