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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.control.WalLaneId;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** RFC 5869 HKDF-SHA-256 derivation with the frozen 37-byte NWG1 Object-key info field. */
public final class ObjectKeyDerivationV1 {
    public static final int RUN_KEY_BYTES = 32;
    public static final int OBJECT_KEY_BYTES = 32;
    public static final int INFO_BYTES = 37;
    private static final byte[] INFO_DOMAIN = "NWG1/OBJ/KEY/V1\0".getBytes(StandardCharsets.US_ASCII);

    private ObjectKeyDerivationV1() {}

    public static CanonicalBytes derive(
            byte[] runKey,
            Sha256Digest rootSha256,
            int shardId,
            long shardRunEpoch,
            WalLaneId laneId,
            long laneSequence) {
        if (runKey == null || runKey.length != RUN_KEY_BYTES) {
            throw new IllegalArgumentException("WalRun plaintext key must be exactly 32 bytes");
        }
        if (rootSha256 == null || rootSha256.isZero() || shardId < 0 || shardRunEpoch < 0 || laneSequence < 0) {
            throw new IllegalArgumentException("Object-key derivation identity is invalid");
        }
        byte[] info = ByteBuffer.allocate(INFO_BYTES)
                .put(INFO_DOMAIN)
                .putInt(shardId)
                .putLong(shardRunEpoch)
                .put((byte) laneId.code())
                .putLong(laneSequence)
                .array();
        byte[] pseudorandomKey = null;
        try {
            Mac hmac = Mac.getInstance("HmacSHA256");
            hmac.init(new SecretKeySpec(rootSha256.bytes().toByteArray(), "HmacSHA256"));
            pseudorandomKey = hmac.doFinal(runKey);
            hmac.init(new SecretKeySpec(pseudorandomKey, "HmacSHA256"));
            hmac.update(info);
            hmac.update((byte) 1);
            byte[] output = hmac.doFinal();
            return CanonicalBytes.copyOf(output);
        } catch (GeneralSecurityException failure) {
            throw new IllegalStateException("JDK has no HKDF HMAC primitive", failure);
        } finally {
            Arrays.fill(info, (byte) 0);
            if (pseudorandomKey != null) {
                Arrays.fill(pseudorandomKey, (byte) 0);
            }
        }
    }
}
