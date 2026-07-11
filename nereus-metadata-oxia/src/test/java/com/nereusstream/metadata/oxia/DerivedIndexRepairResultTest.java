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

package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StreamId;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DerivedIndexRepairResultTest {
    private static final StreamId STREAM_ID = new StreamId("stream");

    @Test
    void continuationIsBoundToStreamTargetAndObservedHead() {
        DerivedIndexRepairCursor cursor = new DerivedIndexRepairCursor(
                STREAM_ID,
                7,
                "head-9",
                9,
                "commit-8",
                8,
                80,
                8);
        DerivedIndexRepairResult result = new DerivedIndexRepairResult(
                STREAM_ID,
                7,
                7,
                1,
                0,
                false,
                true,
                Optional.of(cursor),
                9);

        assertThat(result.continuation()).contains(cursor);
        assertThat(cursor.targetOffset()).isEqualTo(7);
    }

    @Test
    void resultRejectsInconsistentBudgetAndContinuationState() {
        DerivedIndexRepairCursor cursor = new DerivedIndexRepairCursor(
                STREAM_ID,
                0,
                "head-2",
                2,
                "commit-1",
                1,
                10,
                1);

        assertThatThrownBy(() -> new DerivedIndexRepairResult(
                        STREAM_ID,
                        0,
                        0,
                        1,
                        0,
                        false,
                        false,
                        Optional.of(cursor),
                        2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DerivedIndexRepairResult(
                        STREAM_ID,
                        0,
                        0,
                        0,
                        1,
                        true,
                        false,
                        Optional.empty(),
                        1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DerivedIndexRepairResult(
                        STREAM_ID,
                        0,
                        0,
                        1,
                        0,
                        false,
                        false,
                        Optional.empty(),
                        1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
