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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Explicit P1-owned business audit references. Missing domain is unknown, and holds never expire by local time. */
public record M5PulsarAuditDomainV1(List<Hold> holds) {
    public static final int MAX_HOLDS = 64;
    public static final int MAX_BYTES = 524_288;
    private static final int MAGIC = 0x4d355041; // M5PA

    public record Hold(
            Sha256Digest obligationId,
            PhysicalResourceIdV2 resource,
            M5PulsarCoverageV2 coverage,
            long admissionGeneration) {
        public Hold {
            M5RetentionRecordsV1.requireDigest(obligationId, "obligationId");
            Objects.requireNonNull(resource, "resource");
            Objects.requireNonNull(coverage, "coverage");
            if (admissionGeneration <= 0) {
                throw new IllegalArgumentException("audit admission generation must be positive");
            }
        }
    }

    public M5PulsarAuditDomainV1 {
        holds = List.copyOf(Objects.requireNonNull(holds, "holds"));
        if (holds.size() > MAX_HOLDS
                || !holds.equals(holds.stream()
                        .sorted(Comparator.comparing(hold -> hold.obligationId().toHex()))
                        .toList())
                || holds.stream().map(Hold::obligationId).distinct().count() != holds.size()) {
            throw new IllegalArgumentException("audit holds must be bounded, sorted and unique");
        }
    }

    public static M5PulsarAuditDomainV1 empty() {
        return new M5PulsarAuditDomainV1(List.of());
    }

    public M5PulsarAuditDomainV1 withHolds(List<Hold> next) {
        return new M5PulsarAuditDomainV1(next);
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(1);
                out.writeInt(holds.size());
                for (var hold : holds) {
                    var resource = hold.resource().canonicalBytes();
                    var coverage = hold.coverage().encode();
                    if ((long) bytes.size() + 48 + resource.length() + coverage.length() > MAX_BYTES) {
                        throw new IllegalArgumentException("audit domain exceeds its value bound");
                    }
                    out.write(hold.obligationId().bytes().toByteArray());
                    out.writeLong(hold.admissionGeneration());
                    write(out, resource);
                    write(out, coverage);
                }
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("audit domain exceeds its value bound");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("cannot encode audit domain", failure);
        }
    }

    public static M5PulsarAuditDomainV1 decode(CanonicalBytes encoded) {
        if (encoded.length() > MAX_BYTES) {
            throw new IllegalArgumentException("audit domain exceeds its value bound");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != 1) {
                throw new IllegalArgumentException("audit domain preamble differs");
            }
            int count = in.readInt();
            if (count < 0 || count > MAX_HOLDS) {
                throw new IllegalArgumentException("audit hold count exceeds its bound");
            }
            var holds = new ArrayList<Hold>();
            for (int i = 0; i < count; i++) {
                var id = in.readNBytes(Sha256Digest.LENGTH);
                long generation = in.readLong();
                holds.add(new Hold(
                        Sha256Digest.copyOf(id),
                        PhysicalResourceIdCodecV2.decode(read(in)),
                        M5PulsarCoverageV2.decode(read(in)),
                        generation));
            }
            var value = new M5PulsarAuditDomainV1(holds);
            if (in.read() != -1 || !value.encode().equals(encoded)) {
                throw new IllegalArgumentException("audit domain is not canonical");
            }
            return value;
        } catch (IOException failure) {
            throw new IllegalArgumentException("truncated audit domain", failure);
        }
    }

    private static void write(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static CanonicalBytes read(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > MAX_BYTES || length > in.available()) {
            throw new IllegalArgumentException("audit field exceeds its remaining value bound");
        }
        return CanonicalBytes.copyOf(in.readNBytes(length));
    }
}
