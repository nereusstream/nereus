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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Pattern;

/** Exact Root-bound, content-addressed NWKCP1 object and same-family Head keys. */
public final class Nwkcp1ObjectKeyV1 {
    private static final Pattern PREFIX = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9._/-]*");

    private Nwkcp1ObjectKeyV1() {}

    public static String objectKey(String walRunPrefix, Sha256Digest objectDigest) {
        Objects.requireNonNull(objectDigest, "objectDigest");
        String prefix = validatePrefix(walRunPrefix);
        return checked(prefix + "/" + Nwkcp1ConstantsV1.FAMILY_PREFIX + "/"
                + Nwkcp1ConstantsV1.OBJECTS_TOKEN + "/" + Nwkcp1ConstantsV1.DIGEST_TOKEN
                + objectDigest.toHex() + Nwkcp1ConstantsV1.OBJECT_SUFFIX);
    }

    public static String headKey(String walRunPrefix) {
        String prefix = validatePrefix(walRunPrefix);
        return checked(prefix + "/" + Nwkcp1ConstantsV1.FAMILY_PREFIX + "/" + Nwkcp1ConstantsV1.HEAD_TOKEN);
    }

    public static Sha256Digest parseObjectDigest(String walRunPrefix, String key) {
        Objects.requireNonNull(key, "key");
        String marker = validatePrefix(walRunPrefix) + "/" + Nwkcp1ConstantsV1.FAMILY_PREFIX + "/"
                + Nwkcp1ConstantsV1.OBJECTS_TOKEN + "/" + Nwkcp1ConstantsV1.DIGEST_TOKEN;
        if (!key.startsWith(marker) || !key.endsWith(Nwkcp1ConstantsV1.OBJECT_SUFFIX)) {
            throw new IllegalArgumentException("NWKCP1 object key is outside the Root-bound family");
        }
        String hex = key.substring(marker.length(), key.length() - Nwkcp1ConstantsV1.OBJECT_SUFFIX.length());
        if (hex.length() != 64 || !hex.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("NWKCP1 object key digest is not canonical lowercase SHA-256");
        }
        return Sha256Digest.copyOf(java.util.HexFormat.of().parseHex(hex));
    }

    static void requireExactObjectKey(String walRunPrefix, String key, Sha256Digest objectDigest) {
        Objects.requireNonNull(objectDigest, "objectDigest");
        Sha256Digest parsed = parseObjectDigest(walRunPrefix, checked(Objects.requireNonNull(key, "key")));
        if (!parsed.equals(objectDigest) || !key.equals(objectKey(walRunPrefix, objectDigest))) {
            throw new IllegalArgumentException("NWKCP1 object key does not name its exact object digest");
        }
    }

    static void requireCanonicalEmbeddedObjectKey(String key, Sha256Digest objectDigest) {
        Objects.requireNonNull(key, "key");
        String marker = "/" + Nwkcp1ConstantsV1.FAMILY_PREFIX + "/" + Nwkcp1ConstantsV1.OBJECTS_TOKEN + "/"
                + Nwkcp1ConstantsV1.DIGEST_TOKEN;
        int markerOffset = key.lastIndexOf(marker);
        if (markerOffset <= 0) {
            throw new IllegalArgumentException("NWKCP1 object key is outside the exact family grammar");
        }
        requireExactObjectKey(key.substring(0, markerOffset), key, objectDigest);
    }

    private static String validatePrefix(String value) {
        Objects.requireNonNull(value, "walRunPrefix");
        if (value.isEmpty()
                || value.startsWith("/")
                || value.endsWith("/")
                || value.contains("//")
                || !PREFIX.matcher(value).matches()) {
            throw new IllegalArgumentException("WalRun prefix is not canonical");
        }
        for (String segment : value.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("WalRun prefix is not canonical");
            }
        }
        return value;
    }

    private static String checked(String key) {
        if (key.getBytes(StandardCharsets.US_ASCII).length > Nwkcp1ConstantsV1.FORMAT_MAX_KEY_BYTES) {
            throw new IllegalArgumentException("NWKCP1 key exceeds its persisted cap");
        }
        return key;
    }
}
