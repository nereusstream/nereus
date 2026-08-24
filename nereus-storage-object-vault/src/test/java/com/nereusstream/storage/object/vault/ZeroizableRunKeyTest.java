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

package com.nereusstream.storage.object.vault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ZeroizableRunKeyTest {
    @Test
    void erasesOwnedStateAndTemporaryCallbackCopy() {
        byte[] source = new byte[32];
        Arrays.fill(source, (byte) 7);
        ZeroizableRunKey key = new ZeroizableRunKey(source);
        Arrays.fill(source, (byte) 0);

        int observedLength = key.use(bytes -> {
            assertThat(bytes).containsOnly((byte) 7);
            bytes[0] = 9;
            return bytes.length;
        });
        byte observedFirst = key.use(bytes -> bytes[0]);
        assertThat(observedLength).isEqualTo(32);
        assertThat(observedFirst).isEqualTo((byte) 7);

        key.close();
        assertThat(key.isDestroyed()).isTrue();
        assertThatThrownBy(() -> key.use(bytes -> bytes.length)).isInstanceOf(IllegalStateException.class);
    }
}
