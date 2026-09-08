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

package com.nereusstream.storage.bookkeeper;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/** One permanent native intent binding per resource; a changed token requires a strictly newer native GC epoch. */
public record M5BookKeeperNativeDeleteIntentV2(
        M5BookKeeperNativeDeleteAuthorityV2.Snapshot epoch,
        Sha256Digest dispatchTokenSha256,
        Sha256Digest intentAuthoritySha256,
        Sha256Digest ledgerMetadataSha256,
        int nativeVersion) {
    private static final int MAGIC = 0x4d354449; // M5DI
    private static final int MAX_BYTES = 65536 + 256;

    public M5BookKeeperNativeDeleteIntentV2 {
        Objects.requireNonNull(epoch, "epoch");
        Objects.requireNonNull(dispatchTokenSha256, "dispatchTokenSha256");
        Objects.requireNonNull(intentAuthoritySha256, "intentAuthoritySha256");
        Objects.requireNonNull(ledgerMetadataSha256, "ledgerMetadataSha256");
        if (nativeVersion < 0
                || dispatchTokenSha256.isZero()
                || intentAuthoritySha256.isZero()
                || ledgerMetadataSha256.isZero()) {
            throw new IllegalArgumentException("native intent version or identity differs");
        }
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(1);
                out.writeInt(nativeVersion);
                out.writeInt(epoch.nativeVersion());
                byte[] epochBytes = epoch.encode().toByteArray();
                out.writeInt(epochBytes.length);
                out.write(epochBytes);
                out.write(dispatchTokenSha256.bytes().toByteArray());
                out.write(intentAuthoritySha256.bytes().toByteArray());
                out.write(ledgerMetadataSha256.bytes().toByteArray());
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("native intent exceeds bound");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("native intent encoding failed", failure);
        }
    }

    static M5BookKeeperNativeDeleteIntentV2 decode(CanonicalBytes bytes, int nativeVersion) {
        if (bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("native intent exceeds bound");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != 1 || in.readInt() != nativeVersion) {
                throw new IllegalArgumentException("native intent preamble/version differs");
            }
            int epochVersion = in.readInt();
            int epochLength = in.readInt();
            if (epochLength <= 0 || epochLength > MAX_BYTES) {
                throw new IllegalArgumentException("native intent epoch length differs");
            }
            var epoch = M5BookKeeperNativeDeleteAuthorityV2.decode(
                    CanonicalBytes.copyOf(in.readNBytes(epochLength)), epochVersion);
            var value = new M5BookKeeperNativeDeleteIntentV2(
                    epoch,
                    Sha256Digest.copyOf(in.readNBytes(Sha256Digest.LENGTH)),
                    Sha256Digest.copyOf(in.readNBytes(Sha256Digest.LENGTH)),
                    Sha256Digest.copyOf(in.readNBytes(Sha256Digest.LENGTH)),
                    nativeVersion);
            if (in.available() != 0 || !value.encode().equals(bytes)) {
                throw new IllegalArgumentException("native intent is noncanonical");
            }
            return value;
        } catch (IOException failure) {
            throw new IllegalArgumentException("native intent is truncated", failure);
        }
    }
}
