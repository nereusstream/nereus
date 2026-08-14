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

package com.nereusstream.kafka.bookkeeper.nbke2;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class Nbke2WireProjectionV1Test {
    private static final Path PROJECTION = Path.of("..", "docs", "v2", "wire", "nbke2-v1.json");

    @Test
    void independentMachineProjectionMatchesEveryProductionConstant() throws Exception {
        String projection = Files.readString(PROJECTION);

        assertThat(projection)
                .contains("\"schema\": \"NEREUS_NBKE2_WIRE_PROJECTION_V1\"")
                .contains("\"magicAscii\": \"NBKE2\"")
                .contains("\"majorVersion\": " + Nbke2ConstantsV1.MAJOR_VERSION)
                .contains("\"minorVersion\": " + Nbke2ConstantsV1.MINOR_VERSION)
                .contains("\"commonHeaderBytes\": " + Nbke2ConstantsV1.FIXED_HEADER_BYTES)
                .contains("\"formatMaxFrameBytes\": " + Nbke2ConstantsV1.FORMAT_MAX_FRAME_BYTES)
                .contains("\"formatMaxDataPayloadBytes\": " + Nbke2ConstantsV1.FORMAT_MAX_DATA_PAYLOAD_BYTES)
                .contains("\"formatMaxTopicNameBytes\": " + Nbke2ConstantsV1.FORMAT_MAX_TOPIC_NAME_BYTES)
                .contains("\"formatMaxLocatorCount\": " + Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT)
                .contains("\"formatMaxIndexDirectoryCount\": " + Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT)
                .contains(
                        "\"formatMaxCheckpointSectionBytes\": " + Nbke2ConstantsV1.FORMAT_MAX_CHECKPOINT_SECTION_BYTES)
                .contains("\"unknownMinorPolicy\": \"REJECT\"")
                .contains("\"strictEof\": true");

        for (Nbke2FrameTypeV1 type : Nbke2FrameTypeV1.values()) {
            assertThat(projection).contains("\"name\": \"" + type.name() + "\", \"code\": " + type.code());
        }
        assertThat(projection)
                .contains("bytes[0,totalLength-4)")
                .contains("bytes[0,shaFieldOffset)")
                .contains("TERMINAL_APPEND_GROUP_DESCRIPTOR_PRESENT")
                .contains("RUN_HEADER header.entryId == 0 and firstDataEntryId > 0")
                .contains("terminal DATA descriptor.lastDataEntryId == header.entryId")
                .contains("RANGE_INDEX_BLOCK lastDataEntryId < header.entryId < successorDataEntryId")
                .contains("RUN_FOOTER lastPhysicalEntryIdExclusive == header.entryId + 1");
    }
}
