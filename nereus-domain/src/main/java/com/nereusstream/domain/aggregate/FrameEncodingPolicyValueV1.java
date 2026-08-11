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

package com.nereusstream.domain.aggregate;

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.util.Objects;

/**
 * An opaque logical frame-policy value with only the frozen NONE/non-NONE shape.
 *
 * <p>This type deliberately assigns no non-NONE kind, version, payload schema, or cap.
 */
public record FrameEncodingPolicyValueV1(int kind, int formatVersion, CanonicalBytes payload) {
    private static final int MAX_U16 = 0xffff;

    public FrameEncodingPolicyValueV1 {
        Objects.requireNonNull(payload, "payload");
        boolean nonePair = kind == 0 && formatVersion == 0;
        boolean nonNonePair = kind >= 1 && kind <= MAX_U16 && formatVersion >= 1 && formatVersion <= MAX_U16;
        if (!nonePair && !nonNonePair) {
            throw new IllegalArgumentException("frame-policy kind/version must be both zero or both in 1..65535");
        }
        if (nonePair && !payload.isEmpty()) {
            throw new IllegalArgumentException("NONE frame policy must have an empty payload");
        }
    }

    public static FrameEncodingPolicyValueV1 none() {
        return new FrameEncodingPolicyValueV1(0, 0, CanonicalBytes.empty());
    }

    public boolean isNone() {
        return kind == 0;
    }
}
