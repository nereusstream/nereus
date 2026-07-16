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

package com.nereusstream.core.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.core.read.GenerationIndexRepairSource;
import com.nereusstream.core.read.ReadAfterStableCommitRepair;
import org.junit.jupiter.api.Test;

class AsyncReadAfterCommitRepairTest {
    @Test
    void committedIndexGapIsRepairedWithVisibleGenerationProtection() {
        GenerationZeroRepairTestSupport.Fixture fixture =
                GenerationZeroRepairTestSupport.fixture(0);
        ReadAfterStableCommitRepair repair =
                new ReadAfterStableCommitRepair(fixture.scanner());

        var result = repair.repair(
                        GenerationZeroRepairTestSupport.STREAM,
                        0,
                        GenerationZeroRepairTestSupport.TIMEOUT)
                .join();

        assertThat(result.source())
                .isEqualTo(GenerationIndexRepairSource.LIVE_COMMIT);
        assertThat(result.scannedRecords()).isOne();
        assertThat(fixture.materializations()).hasValue(1);
        assertThat(fixture.protections()).hasValue(1);
    }

    @Test
    void trimWinsBeforeRepairAndCreatesNoPhysicalReference() {
        GenerationZeroRepairTestSupport.Fixture fixture =
                GenerationZeroRepairTestSupport.fixture(1);
        ReadAfterStableCommitRepair repair =
                new ReadAfterStableCommitRepair(fixture.scanner());

        var result = repair.repair(
                        GenerationZeroRepairTestSupport.STREAM,
                        0,
                        GenerationZeroRepairTestSupport.TIMEOUT)
                .join();

        assertThat(result.source())
                .isEqualTo(GenerationIndexRepairSource.TRIMMED);
        assertThat(fixture.tailReads()).hasValue(0);
        assertThat(fixture.materializations()).hasValue(0);
        assertThat(fixture.protections()).hasValue(0);
    }
}
