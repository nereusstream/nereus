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

import org.junit.jupiter.api.Test;

class GenerationZeroRepairScannerTest {
    @Test
    void repairsAndProtectsEveryUntrimmedLiveCommitFromStableHeadEvidence() {
        GenerationZeroRepairTestSupport.Fixture fixture =
                GenerationZeroRepairTestSupport.fixture(0);

        GenerationZeroRepairScanner.ScanResult result =
                fixture.scanner()
                        .repairAll(
                                GenerationZeroRepairTestSupport.STREAM,
                                GenerationZeroRepairTestSupport.TIMEOUT)
                        .join();

        assertThat(result.streamId())
                .isEqualTo(GenerationZeroRepairTestSupport.STREAM);
        assertThat(result.scannedCommits()).isOne();
        assertThat(result.protectedIndexes()).isOne();
        assertThat(result.anchorReached()).isTrue();
        assertThat(result.observedHeadCommitVersion()).isOne();
        assertThat(fixture.tailReads()).hasValue(1);
        assertThat(fixture.materializations()).hasValue(1);
        assertThat(fixture.protections()).hasValue(1);
    }

    @Test
    void fullyTrimmedLiveCommitCreatesNoNewIndexOrProtection() {
        GenerationZeroRepairTestSupport.Fixture fixture =
                GenerationZeroRepairTestSupport.fixture(1);

        GenerationZeroRepairScanner.ScanResult result =
                fixture.scanner()
                        .repairAll(
                                GenerationZeroRepairTestSupport.STREAM,
                                GenerationZeroRepairTestSupport.TIMEOUT)
                        .join();

        assertThat(result.scannedCommits()).isOne();
        assertThat(result.protectedIndexes()).isZero();
        assertThat(fixture.materializations()).hasValue(0);
        assertThat(fixture.protections()).hasValue(0);
    }
}
