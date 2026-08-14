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

package com.nereusstream.pulsar.offload;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/** Deterministic ADR-0029 key derivation beneath one persisted Cell Provider Scope. */
public record PulsarOffloadKeysV1(String attemptPrefix, String dataKey, String rootKey) {
    public static final int KEY_DERIVATION_VERSION = 1;
    public static final int MAX_SCOPE_BYTES = 768;
    public static final int MAX_KEY_BYTES = 1_024;

    private static final Pattern SCOPE_SEGMENT = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");

    public PulsarOffloadKeysV1 {
        requireKey(attemptPrefix, true);
        requireKey(dataKey, false);
        requireKey(rootKey, false);
        if (!dataKey.equals(attemptPrefix + "/data") || !rootKey.equals(attemptPrefix + "/root")) {
            throw new IllegalArgumentException("attempt keys differ from derivation v1");
        }
    }

    public static PulsarOffloadKeysV1 derive(String providerScopePrefix, long ledgerId, UUID attemptUuid) {
        if (ledgerId < 0) {
            throw new IllegalArgumentException("ledgerId must be non-negative");
        }
        Objects.requireNonNull(attemptUuid, "attemptUuid");
        String scope = canonicalScope(providerScopePrefix);
        String prefix = scope + "/pulsar-offload/v1/ledger-" + ledgerId + "/attempt-" + attemptUuid.toString();
        return new PulsarOffloadKeysV1(prefix, prefix + "/data", prefix + "/root");
    }

    private static String canonicalScope(String input) {
        Objects.requireNonNull(input, "providerScopePrefix");
        if (input.isEmpty()
                || input.startsWith("/")
                || input.endsWith("/")
                || input.contains("//")
                || input.contains("\\")
                || input.getBytes(StandardCharsets.UTF_8).length > MAX_SCOPE_BYTES) {
            throw new IllegalArgumentException("provider scope prefix is not canonical");
        }
        for (String segment : input.split("/")) {
            if (!SCOPE_SEGMENT.matcher(segment).matches() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("provider scope segment is not canonical");
            }
        }
        return input;
    }

    private static void requireKey(String key, boolean prefix) {
        Objects.requireNonNull(key, prefix ? "attemptPrefix" : "key");
        int bytes = key.getBytes(StandardCharsets.UTF_8).length;
        if (bytes == 0 || bytes > MAX_KEY_BYTES || key.startsWith("/") || key.contains("//") || key.contains("\\")) {
            throw new IllegalArgumentException("derived key is outside canonical bounds");
        }
    }
}
