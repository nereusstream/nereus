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

package com.nereusstream.managedledger.entry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.PayloadFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class EncodedAppendTest {
    @Test
    void ownsCallbackBytesOnConstructionAndAccess() {
        byte[] source = new byte[] {1, 2};
        EncodedAppend encoded = new EncodedAppend(batch(source), source, 1);

        source[0] = 9;
        byte[] returned = encoded.callbackBytes();
        returned[1] = 9;

        assertThat(encoded.callbackBytes()).containsExactly(1, 2);
        assertThatThrownBy(() -> new EncodedAppend(batch(new byte[0]), new byte[0], 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static AppendBatch batch(byte[] payload) {
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(new AppendEntry(payload, 1, 0, Map.of())),
                1,
                1,
                0,
                0,
                List.of(),
                Map.of(),
                Optional.empty());
    }
}
