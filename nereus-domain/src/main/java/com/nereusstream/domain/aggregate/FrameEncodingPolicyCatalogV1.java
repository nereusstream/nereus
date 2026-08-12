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

/** The complete closed FrameEncodingPolicy catalog accepted by NTA1 v1. */
public final class FrameEncodingPolicyCatalogV1 {
    public static final int NONE_KIND = 0;
    public static final int NONE_VERSION = 0;
    public static final int ZSTD_FAST_IF_SMALLER_KIND = 1;
    public static final int ZSTD_FAST_IF_SMALLER_VERSION = 1;

    private static final FrameEncodingPolicyValueV1 NONE = FrameEncodingPolicyValueV1.none();
    private static final FrameEncodingPolicyValueV1 ZSTD_FAST_IF_SMALLER = new FrameEncodingPolicyValueV1(
            ZSTD_FAST_IF_SMALLER_KIND, ZSTD_FAST_IF_SMALLER_VERSION, CanonicalBytes.empty());

    private FrameEncodingPolicyCatalogV1() {}

    public static FrameEncodingPolicyValueV1 none() {
        return NONE;
    }

    public static FrameEncodingPolicyValueV1 zstdFastIfSmaller() {
        return ZSTD_FAST_IF_SMALLER;
    }

    public static FrameEncodingPolicyValueV1 fromCodes(int kind, int formatVersion) {
        if (kind == NONE_KIND && formatVersion == NONE_VERSION) {
            return NONE;
        }
        if (kind == ZSTD_FAST_IF_SMALLER_KIND && formatVersion == ZSTD_FAST_IF_SMALLER_VERSION) {
            return ZSTD_FAST_IF_SMALLER;
        }
        throw new IllegalArgumentException("unknown NTA1 v1 frame-policy kind/version: " + kind + "/" + formatVersion);
    }

    public static void validate(FrameEncodingPolicyValueV1 value) {
        Objects.requireNonNull(value, "value");
        FrameEncodingPolicyValueV1 accepted = fromCodes(value.kind(), value.formatVersion());
        if (!value.payload().isEmpty() || !accepted.equals(value)) {
            throw new IllegalArgumentException("NTA1 v1 frame-policy payload must be empty");
        }
    }

    public static FrameEncodingPolicyValueV1 requiredFor(StorageProfileV1 profile) {
        Objects.requireNonNull(profile, "profile");
        return switch (profile) {
            case OBJECT_WAL -> ZSTD_FAST_IF_SMALLER;
            case BOOKKEEPER_WAL_ONLY, BOOKKEEPER_WAL_ASYNC_OBJECT -> NONE;
        };
    }
}
