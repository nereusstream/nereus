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
import java.util.Objects;

/** Provider behavior and checked transfer caps fixed for the lifetime of one Root. */
public record ObjectProviderRootConfiguration(
        ObjectProviderAccessProfile accessProfile,
        String adapterVersion,
        String canonicalizerVersion,
        String exclusiveNamespacePrefix,
        ProviderProofMode proofMode,
        int proofTokenHardCap,
        long maxObjectBodyBytes,
        long maxSinglePutBytes,
        int maxSingleRangeReadBytes,
        int maxPrefixSegmentsPerExtent,
        int maxListPageKeys,
        Sha256Digest capabilityReceiptSha256) {
    public static final int FORMAT_MAX_PREFIX_BYTES = 4 * 1024 * 1024;

    public ObjectProviderRootConfiguration {
        Objects.requireNonNull(accessProfile, "accessProfile");
        requireBoundedUtf8(adapterVersion, "adapterVersion", 128);
        requireBoundedUtf8(canonicalizerVersion, "canonicalizerVersion", 128);
        requireExactPrefix(exclusiveNamespacePrefix);
        Objects.requireNonNull(proofMode, "proofMode");
        Objects.requireNonNull(capabilityReceiptSha256, "capabilityReceiptSha256");
        if (proofTokenHardCap < 0 || proofTokenHardCap > 65_535) {
            throw new IllegalArgumentException("proofTokenHardCap must fit unsigned 16-bit length");
        }
        if (proofMode == ProviderProofMode.NONE && proofTokenHardCap != 0) {
            throw new IllegalArgumentException("NONE proof mode must have a zero token cap");
        }
        if (maxObjectBodyBytes <= 0
                || maxSinglePutBytes <= 0
                || maxSinglePutBytes < maxObjectBodyBytes
                || maxSingleRangeReadBytes <= 0
                || maxPrefixSegmentsPerExtent <= 0
                || maxListPageKeys <= 0) {
            throw new IllegalArgumentException("provider transfer caps must be positive");
        }
        if (accessProfile == ObjectProviderAccessProfile.C1_SINGLE_PUT_SINGLE_RANGE_STRONG_LIST
                && maxPrefixSegmentsPerExtent != 1) {
            throw new IllegalArgumentException("C1 Provider profile requires exactly one prefix segment");
        }
        if (capabilityReceiptSha256.isZero()) {
            throw new IllegalArgumentException("Provider capability receipt SHA-256 must be non-zero");
        }
    }

    private static void requireExactPrefix(String value) {
        requireBoundedUtf8(value, "exclusiveNamespacePrefix", 512);
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        if (bytes.length != value.length() || value.startsWith("/") || value.endsWith("/") || value.contains("//")) {
            throw new IllegalArgumentException("exclusiveNamespacePrefix must be a canonical relative ASCII path");
        }
        for (String segment : value.split("/", -1)) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("exclusiveNamespacePrefix contains a forbidden path segment");
            }
            for (int index = 0; index < segment.length(); index++) {
                char valueChar = segment.charAt(index);
                if (!Character.isLetterOrDigit(valueChar) && valueChar != '.' && valueChar != '_' && valueChar != '-') {
                    throw new IllegalArgumentException("exclusiveNamespacePrefix contains a non-canonical character");
                }
            }
        }
    }

    private static void requireBoundedUtf8(String value, String name, int maximumBytes) {
        Objects.requireNonNull(value, name);
        int length = value.getBytes(StandardCharsets.UTF_8).length;
        if (length == 0 || length > maximumBytes || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be non-empty bounded UTF-8 without NUL");
        }
    }
}
