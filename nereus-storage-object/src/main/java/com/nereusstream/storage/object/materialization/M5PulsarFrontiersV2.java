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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/** Declared native ledger boundaries. They carry no Kafka HW/LSO and do not qualify their source authority. */
public record M5PulsarFrontiersV2(
        LedgerBoundary confirmed, Optional<LedgerBoundary> trim, Optional<LedgerBoundary> recovery) {
    public M5PulsarFrontiersV2 {
        Objects.requireNonNull(confirmed, "confirmed");
        Objects.requireNonNull(trim, "trim");
        Objects.requireNonNull(recovery, "recovery");
    }

    /** Native last-confirmed + 1 within this ledger; zero represents an empty ledger, never a global offset. */
    public record LedgerBoundary(long ledgerId, long entryEndExclusive) {
        public LedgerBoundary {
            if (ledgerId < 0 || entryEndExclusive < 0) {
                throw new IllegalArgumentException("Pulsar ledger boundary is negative");
            }
        }

        M5PulsarCoverageV2.Position position() {
            return new M5PulsarCoverageV2.Position(ledgerId, entryEndExclusive);
        }
    }

    public void requireCut(M5PulsarCoverageV2 cut) {
        if (cut.compare(confirmed.position(), cut.endPosition()) < 0
                || trim.filter(value -> cut.compare(value.position(), cut.firstPosition()) > 0)
                        .isPresent()
                || recovery.filter(value -> cut.compare(value.position(), cut.endPosition()) < 0)
                        .isPresent()) {
            throw new IllegalArgumentException("Pulsar frontiers do not bound the sealed cut");
        }
    }

    void write(DataOutputStream out) throws IOException {
        writeBoundary(out, confirmed);
        writeOptional(out, trim);
        writeOptional(out, recovery);
    }

    static M5PulsarFrontiersV2 read(DataInputStream in) throws IOException {
        return new M5PulsarFrontiersV2(readBoundary(in), readOptional(in), readOptional(in));
    }

    private static void writeBoundary(DataOutputStream out, LedgerBoundary value) throws IOException {
        out.writeLong(value.ledgerId());
        out.writeLong(value.entryEndExclusive());
    }

    private static LedgerBoundary readBoundary(DataInputStream in) throws IOException {
        return new LedgerBoundary(in.readLong(), in.readLong());
    }

    private static void writeOptional(DataOutputStream out, Optional<LedgerBoundary> value) throws IOException {
        out.writeByte(value.isPresent() ? 1 : 0);
        if (value.isPresent()) {
            writeBoundary(out, value.orElseThrow());
        }
    }

    private static Optional<LedgerBoundary> readOptional(DataInputStream in) throws IOException {
        int present = in.readUnsignedByte();
        if (present > 1) {
            throw new IllegalArgumentException("Pulsar frontier presence flag is unknown");
        }
        return present == 1 ? Optional.of(readBoundary(in)) : Optional.empty();
    }
}
