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

package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ObjectPutRetryPolicyTest {
    @Test
    void computesSaturatingCappedExponentialBackoff() {
        ObjectPutRetryPolicy policy = new ObjectPutRetryPolicy(
                10, Duration.ofMillis(25), Duration.ofMillis(100));

        assertThat(policy.maximumBackoffMillis(2)).isEqualTo(25);
        assertThat(policy.maximumBackoffMillis(3)).isEqualTo(50);
        assertThat(policy.maximumBackoffMillis(4)).isEqualTo(100);
        assertThat(policy.maximumBackoffMillis(10)).isEqualTo(100);
    }

    @Test
    void rejectsInvalidAttemptAndDurationBounds() {
        assertThatThrownBy(() -> new ObjectPutRetryPolicy(
                        0, Duration.ofMillis(1), Duration.ofMillis(2)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObjectPutRetryPolicy(
                        11, Duration.ofMillis(1), Duration.ofMillis(2)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObjectPutRetryPolicy(
                        2, Duration.ZERO, Duration.ofMillis(2)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObjectPutRetryPolicy(
                        2, Duration.ofMillis(3), Duration.ofMillis(2)))
                .isInstanceOf(IllegalArgumentException.class);

        ObjectPutRetryPolicy policy = new ObjectPutRetryPolicy(
                2, Duration.ofMillis(1), Duration.ofMillis(2));
        assertThatThrownBy(() -> policy.maximumBackoffMillis(1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> policy.maximumBackoffMillis(3))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
