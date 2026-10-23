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

package com.nereusstream.storage.object.materialization;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** The concrete shared coverage projection for M5 control, NMS1 and lookup records. Kafka v1 bytes stay exact. */
final class M5CoverageWireV2 {
    private M5CoverageWireV2() {}

    static int version(ProtocolCoverage coverage) {
        return coverage.typedPulsar() ? 2 : 1;
    }

    static void writeFull(DataOutputStream out, ProtocolCoverage value) throws IOException {
        out.writeByte(value.domain().ordinal());
        if (value.typedPulsar()) {
            var bytes = value.pulsarCoverage().orElseThrow().encode();
            out.writeInt(bytes.length());
            out.write(bytes.toByteArray());
        } else {
            out.writeLong(value.inclusiveStart());
            out.writeLong(value.exclusiveEnd());
        }
    }

    static ProtocolCoverage readFull(DataInputStream in, int version) throws IOException {
        int domain = in.readUnsignedByte();
        if (version == 2) {
            if (domain != PositionDomain.PULSAR_ENTRY.ordinal()) {
                throw new IllegalArgumentException("M5 v2 requires ledger-keyed Pulsar coverage");
            }
            int length = in.readInt();
            if (length <= 0 || length > M5PulsarCoverageV2.MAX_BYTES) {
                throw new IllegalArgumentException("Pulsar coverage exceeds its byte cap");
            }
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return ProtocolCoverage.pulsar(M5PulsarCoverageV2.decode(CanonicalBytes.copyOf(bytes)));
        }
        if (version != 1 || domain >= PositionDomain.values().length) {
            throw new IllegalArgumentException("M5 coverage version/domain is unknown");
        }
        return new ProtocolCoverage(PositionDomain.values()[domain], in.readLong(), in.readLong());
    }

    static void writeChild(DataOutputStream out, ProtocolCoverage value, ProtocolCoverage context) throws IOException {
        if (context.typedPulsar()) {
            if (!value.typedPulsar()
                    || !context.pulsarCoverage()
                            .orElseThrow()
                            .sameChain(value.pulsarCoverage().orElseThrow())) {
                throw new IllegalArgumentException("Pulsar range reference belongs to another context");
            }
            value.pulsarCoverage().orElseThrow().writeRangeReference(out);
        } else {
            if (value.typedPulsar()) {
                throw new IllegalArgumentException("Pulsar range reference has no parent context");
            }
            writeFull(out, value);
        }
    }

    static ProtocolCoverage readChild(DataInputStream in, ProtocolCoverage context) throws IOException {
        return context.typedPulsar()
                ? ProtocolCoverage.pulsar(M5PulsarCoverageV2.readRangeReference(
                        in, context.pulsarCoverage().orElseThrow()))
                : readFull(in, 1);
    }
}
