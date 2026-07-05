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

import io.nereus.api.StreamId;
import io.nereus.api.StreamName;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Shared deterministic identity helpers used by metadata and object modules. */
public final class DeterministicIds {
    private DeterministicIds() {
    }

    public static StreamId streamIdFor(StreamName name) {
        return new StreamId("s-" + streamNameHash(name));
    }

    public static String streamNameHash(StreamName name) {
        Objects.requireNonNull(name, "name");
        return sha256Base32(name.value().getBytes(StandardCharsets.UTF_8));
    }

    public static String stableHashComponent(String value) {
        Objects.requireNonNull(value, "value");
        return sha256Base32(value.getBytes(StandardCharsets.UTF_8));
    }

    public static String randomRunIdHash(byte[] randomBytes) {
        Objects.requireNonNull(randomBytes, "randomBytes");
        if (randomBytes.length < 16) {
            throw new IllegalArgumentException("randomBytes must contain at least 128 bits of entropy");
        }
        return sha256Base32(randomBytes.clone());
    }

    private static String sha256Base32(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return Base32LowerNoPad.encode(digest.digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
