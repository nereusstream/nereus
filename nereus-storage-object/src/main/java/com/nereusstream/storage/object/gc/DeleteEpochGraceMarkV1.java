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

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Persisted epoch-clock identity and selected policy; local ticks are never persisted or compared across epochs. */
public record DeleteEpochGraceMarkV1(
        UUID clockDomain,
        DeleteObservationContextV2 authority,
        long durationMillis,
        Sha256Digest initialObservationRootSha256) {
    private static final int MAGIC = 0x4d354547; // M5EG
    public static final int MAX_BYTES = 16_384;

    public DeleteEpochGraceMarkV1 {
        Objects.requireNonNull(clockDomain, "clockDomain");
        Objects.requireNonNull(authority, "authority");
        M5TargetDeleteAuthorityRecordsV1.requireDigest(initialObservationRootSha256, "initialObservationRootSha256");
        if (clockDomain.equals(new UUID(0, 0)) || durationMillis <= 0 || durationMillis > Long.MAX_VALUE / 1_000_000) {
            throw new IllegalArgumentException("epoch grace clock identity or finite positive duration differs");
        }
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(1);
                out.writeLong(clockDomain.getMostSignificantBits());
                out.writeLong(clockDomain.getLeastSignificantBits());
                out.writeLong(authority.observationEpoch());
                DeleteEligibilityCodecV2.writeFact(out, authority.coordinatorOwner());
                DeleteEligibilityCodecV2.writeFact(out, authority.capability());
                out.writeBoolean(authority.predecessorOwnerFenced().isPresent());
                if (authority.predecessorOwnerFenced().isPresent()) {
                    DeleteEligibilityCodecV2.writeFact(
                            out, authority.predecessorOwnerFenced().orElseThrow());
                }
                out.writeLong(durationMillis);
                out.write(initialObservationRootSha256.bytes().toByteArray());
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("epoch grace mark exceeds bound");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("epoch grace encoding failed", failure);
        }
    }

    public static DeleteEpochGraceMarkV1 decode(CanonicalBytes bytes) {
        if (bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("epoch grace mark exceeds bound");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != 1) {
                throw new IllegalArgumentException("epoch grace mark preamble differs");
            }
            var domain = new UUID(in.readLong(), in.readLong());
            var epoch = in.readLong();
            var owner = DeleteEligibilityCodecV2.readFact(in);
            var capability = DeleteEligibilityCodecV2.readFact(in);
            var fenced = in.readBoolean()
                    ? Optional.of(DeleteEligibilityCodecV2.readFact(in))
                    : Optional.<com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1>empty();
            var mark = new DeleteEpochGraceMarkV1(
                    domain,
                    new DeleteObservationContextV2(epoch, owner, capability, fenced),
                    in.readLong(),
                    Sha256Digest.copyOf(in.readNBytes(Sha256Digest.LENGTH)));
            if (in.available() != 0 || !mark.encode().equals(bytes)) {
                throw new IllegalArgumentException("epoch grace mark is noncanonical");
            }
            return mark;
        } catch (IOException failure) {
            throw new IllegalArgumentException("epoch grace mark is truncated", failure);
        }
    }
}
