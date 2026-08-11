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

/** The complete V1 profile-origin discriminator table. */
public enum ProfileOriginV1 {
    DEPLOYMENT_USER_DEFAULT(1),
    TENANT_OVERRIDE(2),
    NAMESPACE_OVERRIDE(3),
    TOPIC_EXPLICIT(4),
    DEPLOYMENT_INTERNAL(5);

    private final int code;

    ProfileOriginV1(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static ProfileOriginV1 fromCode(int code) {
        return switch (code) {
            case 1 -> DEPLOYMENT_USER_DEFAULT;
            case 2 -> TENANT_OVERRIDE;
            case 3 -> NAMESPACE_OVERRIDE;
            case 4 -> TOPIC_EXPLICIT;
            case 5 -> DEPLOYMENT_INTERNAL;
            default -> throw new IllegalArgumentException("unknown ProfileOriginV1 code: " + code);
        };
    }
}
