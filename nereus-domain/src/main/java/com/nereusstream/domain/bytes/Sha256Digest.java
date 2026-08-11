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

package com.nereusstream.domain.bytes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** An immutable 32-byte SHA-256 value. */
public final class Sha256Digest {
    public static final int LENGTH = 32;

    private final CanonicalBytes bytes;

    private Sha256Digest(CanonicalBytes bytes) {
        this.bytes = bytes;
    }

    public static Sha256Digest copyOf(byte[] value) {
        Objects.requireNonNull(value, "value");
        if (value.length != LENGTH) {
            throw new IllegalArgumentException("SHA-256 digest must be exactly 32 bytes");
        }
        return new Sha256Digest(CanonicalBytes.copyOf(value));
    }

    public static Sha256Digest hash(CanonicalBytes value) {
        Objects.requireNonNull(value, "value");
        try {
            return copyOf(MessageDigest.getInstance("SHA-256").digest(value.toByteArray()));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK has no SHA-256 provider", e);
        }
    }

    public CanonicalBytes bytes() {
        return bytes;
    }

    public boolean isZero() {
        byte[] value = bytes.toByteArray();
        int aggregate = 0;
        for (byte item : value) {
            aggregate |= item;
        }
        return aggregate == 0;
    }

    public String toHex() {
        return bytes.toHex();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof Sha256Digest that && bytes.equals(that.bytes);
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    @Override
    public String toString() {
        return toHex();
    }
}
