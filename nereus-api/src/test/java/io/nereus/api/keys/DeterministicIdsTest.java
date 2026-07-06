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

package io.nereus.api.keys;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.nereus.api.StreamId;
import io.nereus.api.StreamName;
import org.junit.jupiter.api.Test;

class DeterministicIdsTest {
    @Test
    void streamIdUsesExactUtf8StreamNameHash() {
        StreamName name = new StreamName("tenant/ns/topic-partition-0");

        assertThat(DeterministicIds.streamNameHash(name))
                .isEqualTo("xronq4iumibg3q6ic7wbkzxex5iky5pzf437l2zsezy5644k2ziq");
        assertThat(DeterministicIds.streamIdFor(name))
                .isEqualTo(new StreamId("s-xronq4iumibg3q6ic7wbkzxex5iky5pzf437l2zsezy5644k2ziq"));
    }

    @Test
    void streamNameWhitespaceIsPartOfTheHashInput() {
        assertThat(DeterministicIds.streamIdFor(new StreamName("topic")))
                .isNotEqualTo(DeterministicIds.streamIdFor(new StreamName(" topic ")));
    }

    @Test
    void randomRunIdHashRequiresAtLeast128BitsOfInputEntropy() {
        assertThatThrownBy(() -> DeterministicIds.randomRunIdHash(new byte[15]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(DeterministicIds.randomRunIdHash(new byte[16]))
                .hasSize(52)
                .matches("[a-z2-7]+");
    }
}
