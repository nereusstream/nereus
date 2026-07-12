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

import com.nereusstream.api.AppendBatch;
import java.util.Objects;

/** Immutable F2 append input plus the exact bytes retained for the Pulsar callback. */
public record EncodedAppend(
        AppendBatch appendBatch,
        byte[] callbackBytes,
        int numberOfMessages) {
    public EncodedAppend {
        Objects.requireNonNull(appendBatch, "appendBatch");
        callbackBytes = Objects.requireNonNull(callbackBytes, "callbackBytes").clone();
        if (numberOfMessages < 1) {
            throw new IllegalArgumentException("numberOfMessages must be positive");
        }
    }

    @Override
    public byte[] callbackBytes() {
        return callbackBytes.clone();
    }
}
