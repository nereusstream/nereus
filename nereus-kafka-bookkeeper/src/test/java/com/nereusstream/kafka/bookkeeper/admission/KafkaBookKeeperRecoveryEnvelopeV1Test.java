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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperRecoveryEnvelopeV1Test {
    private static final KafkaBookKeeperRecoveryEnvelopeV1 ENVELOPE =
            new KafkaBookKeeperRecoveryEnvelopeV1(10, 100, 1_000);

    @Test
    void entryCountIsInclusiveAndProvenOneBeforeAtAndOneAfter() {
        assertThat(classify(9, 0, 0)).isEqualTo(KafkaBookKeeperRecoveryStatusV1.WITHIN_ENVELOPE);
        assertThat(classify(10, 0, 0)).isEqualTo(KafkaBookKeeperRecoveryStatusV1.WITHIN_ENVELOPE);
        assertThat(classify(11, 0, 0)).isEqualTo(KafkaBookKeeperRecoveryStatusV1.ENTRY_COUNT_EXCEEDED);
    }

    @Test
    void encodedBytesAndElapsedTimeAreEachInclusiveAndMandatory() {
        assertThat(classify(0, 99, 999)).isEqualTo(KafkaBookKeeperRecoveryStatusV1.WITHIN_ENVELOPE);
        assertThat(classify(0, 100, 1_000)).isEqualTo(KafkaBookKeeperRecoveryStatusV1.WITHIN_ENVELOPE);
        assertThat(classify(0, 101, 0)).isEqualTo(KafkaBookKeeperRecoveryStatusV1.ENCODED_BYTES_EXCEEDED);
        assertThat(classify(0, 0, 1_001)).isEqualTo(KafkaBookKeeperRecoveryStatusV1.ELAPSED_NANOS_EXCEEDED);
    }

    @Test
    void lowerAuthorityMayOnlyReduceEveryDimension() {
        KafkaBookKeeperRecoveryEnvelopeV1 lowered =
                ENVELOPE.loweredBy(new KafkaBookKeeperRecoveryEnvelopeV1(9, 90, 900));

        assertThat(lowered).isEqualTo(new KafkaBookKeeperRecoveryEnvelopeV1(9, 90, 900));
        assertThatThrownBy(() -> ENVELOPE.loweredBy(new KafkaBookKeeperRecoveryEnvelopeV1(11, 90, 900)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot enlarge");
        assertThatThrownBy(() -> ENVELOPE.loweredBy(new KafkaBookKeeperRecoveryEnvelopeV1(9, 101, 900)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot enlarge");
        assertThatThrownBy(() -> ENVELOPE.loweredBy(new KafkaBookKeeperRecoveryEnvelopeV1(9, 90, 1_001)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot enlarge");
    }

    @Test
    void progressUsesCheckedNonNegativeAccumulation() {
        KafkaBookKeeperRecoveryProgressV1 progress =
                KafkaBookKeeperRecoveryProgressV1.ZERO.advance(1, 2, 3).advance(4, 5, 6);

        assertThat(progress).isEqualTo(new KafkaBookKeeperRecoveryProgressV1(5, 7, 9));
        assertThatThrownBy(() -> progress.advance(-1, 0, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaBookKeeperRecoveryProgressV1(Long.MAX_VALUE, 0, 0).advance(1, 0, 0))
                .isInstanceOf(ArithmeticException.class);
    }

    private static KafkaBookKeeperRecoveryStatusV1 classify(long entries, long bytes, long nanos) {
        return ENVELOPE.classify(new KafkaBookKeeperRecoveryProgressV1(entries, bytes, nanos));
    }
}
