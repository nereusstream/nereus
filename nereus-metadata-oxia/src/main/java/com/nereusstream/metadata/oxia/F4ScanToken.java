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

package com.nereusstream.metadata.oxia;

import java.util.Objects;

/** Opaque, process-local continuation bound to one exact Phase 4 scan scope. */
public final class F4ScanToken {
    private final String cluster;
    private final F4ScanKind kind;
    private final String scopeIdentitySha256;
    private final String scanPrefix;
    private final String exclusiveLastKey;

    F4ScanToken(
            String cluster,
            F4ScanKind kind,
            String scopeIdentitySha256,
            String scanPrefix,
            String exclusiveLastKey) {
        this.cluster = requireText(cluster, "cluster");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.scopeIdentitySha256 = requireSha256(scopeIdentitySha256, "scopeIdentitySha256");
        this.scanPrefix = requireText(scanPrefix, "scanPrefix");
        this.exclusiveLastKey = requireText(exclusiveLastKey, "exclusiveLastKey");
        if (!exclusiveLastKey.startsWith(scanPrefix)) {
            throw new IllegalArgumentException("exclusiveLastKey must be inside scanPrefix");
        }
    }

    String cluster() {
        return cluster;
    }

    F4ScanKind kind() {
        return kind;
    }

    String scopeIdentitySha256() {
        return scopeIdentitySha256;
    }

    String scanPrefix() {
        return scanPrefix;
    }

    String exclusiveLastKey() {
        return exclusiveLastKey;
    }

    String resumeFromInclusive() {
        return exclusiveLastKey + '\0';
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }

    private static String requireSha256(String value, String name) {
        String result = requireText(value, name);
        if (result.length() != 64) {
            throw new IllegalArgumentException(name + " must be lowercase SHA-256");
        }
        for (int index = 0; index < result.length(); index++) {
            char character = result.charAt(index);
            if (!((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f'))) {
                throw new IllegalArgumentException(name + " must be lowercase SHA-256");
            }
        }
        return result;
    }
}
