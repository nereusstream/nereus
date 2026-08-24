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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Exact production leaf-key grammar below one Root-exclusive Provider prefix. */
public record ObjectWalLeafKeyV1(
        WalLaneId laneId, long laneSequence, int directoryPrefixEnd, long bodyLength, Sha256Digest objectSha256) {
    public static final int RELATIVE_KEY_BYTES = 140;
    public static final int ROOT_SEPARATOR_BYTES = 1;
    public static final int ROOT_SUFFIX_BYTES = ROOT_SEPARATOR_BYTES + RELATIVE_KEY_BYTES;

    private static final Pattern RELATIVE =
            Pattern.compile("([0-2])/([0-9]{19})/([0-9]{19})-([0-9]{19})-sha256-v1-([0-9a-f]{64})\\.nwg");

    public ObjectWalLeafKeyV1 {
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(objectSha256, "objectSha256");
        if (laneSequence < 0 || directoryPrefixEnd <= 0 || bodyLength <= 0) {
            throw new IllegalArgumentException("leaf sequence, prefix end, and body length must be positive/bounded");
        }
        if (directoryPrefixEnd > ObjectProviderRootConfiguration.FORMAT_MAX_PREFIX_BYTES
                || directoryPrefixEnd > bodyLength) {
            throw new IllegalArgumentException("leaf directory prefix lies outside the Object body");
        }
        if (objectSha256.isZero()) {
            throw new IllegalArgumentException("leaf Object SHA-256 must be non-zero");
        }
    }

    public static ObjectWalLeafKeyV1 fromRow(ProviderResolvedExtentRowV1 row) {
        Objects.requireNonNull(row, "row");
        return new ObjectWalLeafKeyV1(
                row.laneId(), row.laneSequence(), row.directoryPrefixEnd(), row.bodyLength(), row.objectSha256());
    }

    public String relativeKey() {
        String value = String.format(
                Locale.ROOT,
                "%s/%019d/%019d-%019d-sha256-v1-%s.nwg",
                laneId.leafToken(),
                laneSequence,
                directoryPrefixEnd,
                bodyLength,
                objectSha256.toHex());
        if (value.getBytes(StandardCharsets.US_ASCII).length != RELATIVE_KEY_BYTES) {
            throw new IllegalStateException("leaf key violated its exact fixed-width grammar");
        }
        return value;
    }

    public String fullKey(ObjectProviderRootConfiguration provider) {
        Objects.requireNonNull(provider, "provider");
        return provider.exclusiveNamespacePrefix() + "/" + relativeKey();
    }

    public static ObjectWalLeafKeyV1 parseRelative(String value) {
        Objects.requireNonNull(value, "value");
        if (value.getBytes(StandardCharsets.UTF_8).length != RELATIVE_KEY_BYTES) {
            throw new IllegalArgumentException("Object WAL leaf key has a non-canonical byte length");
        }
        Matcher matcher = RELATIVE.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Object WAL leaf key violates the exact grammar");
        }
        try {
            return new ObjectWalLeafKeyV1(
                    WalLaneId.fromCode(Integer.parseInt(matcher.group(1))),
                    Long.parseLong(matcher.group(2)),
                    Math.toIntExact(Long.parseLong(matcher.group(3))),
                    Long.parseLong(matcher.group(4)),
                    Sha256Digest.copyOf(HexFormat.of().parseHex(matcher.group(5))));
        } catch (ArithmeticException | NumberFormatException exception) {
            throw new IllegalArgumentException("Object WAL leaf key contains an out-of-domain integer", exception);
        }
    }

    public static ObjectWalLeafKeyV1 parseFull(ObjectProviderRootConfiguration provider, String value) {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(value, "value");
        String exactPrefix = provider.exclusiveNamespacePrefix() + "/";
        if (!value.startsWith(exactPrefix)) {
            throw new IllegalArgumentException("Object WAL leaf key is outside the Root-exclusive prefix");
        }
        return parseRelative(value.substring(exactPrefix.length()));
    }

    public static int maximumFullKeyBytes(ObjectProviderRootConfiguration provider) {
        Objects.requireNonNull(provider, "provider");
        return Math.addExact(
                provider.exclusiveNamespacePrefix().getBytes(StandardCharsets.US_ASCII).length, ROOT_SUFFIX_BYTES);
    }
}
