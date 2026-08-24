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

package com.nereusstream.storage.object.kms;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.nwg1.Nwg1EnvelopeV1;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Exact closed KMS_WRAPPED_WALRUN_KEY_V1 envelope persisted in one immutable WalRun Root. */
public record WrappedRunKeyEnvelope(
        String providerId,
        String wrappingAlgorithmId,
        String wrappingKeyId,
        String wrappingKeyVersion,
        CanonicalBytes wrappedKey) {
    public static final int KIND = Nwg1EnvelopeV1.KIND;
    public static final int VERSION = Nwg1EnvelopeV1.VERSION;
    public static final int MAX_WRAPPED_KEY_BYTES = 16 * 1024;
    public static final int MAX_FRAMED_BYTES = 8 + 20 + 64 + 64 + 4_096 + 1_024 + MAX_WRAPPED_KEY_BYTES;

    public WrappedRunKeyEnvelope {
        Objects.requireNonNull(wrappedKey, "wrappedKey");
        new Nwg1EnvelopeV1(
                ascii(providerId, "providerId"),
                ascii(wrappingAlgorithmId, "wrappingAlgorithmId"),
                ascii(wrappingKeyId, "wrappingKeyId"),
                ascii(wrappingKeyVersion, "wrappingKeyVersion"),
                wrappedKey.toByteArray());
    }

    public CanonicalBytes canonicalBytes() {
        return CanonicalBytes.copyOf(asNwg1Envelope().canonicalBytes());
    }

    public CanonicalBytes framedBytes() {
        return CanonicalBytes.copyOf(asNwg1Envelope().framedBytes());
    }

    public static WrappedRunKeyEnvelope decodeFramed(CanonicalBytes framed) {
        Objects.requireNonNull(framed, "framed");
        if (framed.length() > MAX_FRAMED_BYTES) {
            throw new IllegalArgumentException("wrapped WalRun key envelope exceeds its hard cap");
        }
        Nwg1EnvelopeV1 decoded = Nwg1EnvelopeV1.decode(framed.toByteArray());
        return new WrappedRunKeyEnvelope(
                string(decoded.providerId()),
                string(decoded.wrappingAlgorithmId()),
                string(decoded.wrappingKeyId()),
                string(decoded.wrappingKeyVersion()),
                CanonicalBytes.copyOf(decoded.wrappedKey()));
    }

    private Nwg1EnvelopeV1 asNwg1Envelope() {
        return new Nwg1EnvelopeV1(
                ascii(providerId, "providerId"),
                ascii(wrappingAlgorithmId, "wrappingAlgorithmId"),
                ascii(wrappingKeyId, "wrappingKeyId"),
                ascii(wrappingKeyVersion, "wrappingKeyVersion"),
                wrappedKey.toByteArray());
    }

    private static byte[] ascii(String value, String name) {
        Objects.requireNonNull(value, name);
        byte[] encoded = value.getBytes(StandardCharsets.US_ASCII);
        if (!new String(encoded, StandardCharsets.US_ASCII).equals(value)) {
            throw new IllegalArgumentException(name + " must be exact ASCII");
        }
        return encoded;
    }

    private static String string(byte[] value) {
        return new String(value, StandardCharsets.US_ASCII);
    }
}
