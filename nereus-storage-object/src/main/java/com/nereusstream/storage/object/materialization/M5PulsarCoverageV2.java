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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Declared ledger-keyed coverage. A strict value/codec, never evidence that the supplied chain or sources are native.
 * Production admission must independently qualify the authority root and every directory/source fact.
 */
public record M5PulsarCoverageV2(
        BindingIdentity binding,
        Sha256Digest chainAuthorityRootSha256,
        Sha256Digest chainSha256,
        List<ChainLedger> chain,
        List<EntryRange> ranges) {
    public static final int MAX_LEDGERS = 256;
    public static final int MAX_RANGES = 256;
    public static final int MAX_BYTES = 65_536;
    private static final int MAGIC = 0x4d355043; // M5PC
    private static final int VERSION = 2;
    private static final int DOMAIN_TAG = 1;
    private static final String CHAIN_DOMAIN = "NEREUS_V2_M5_PULSAR_LEDGER_CHAIN_V2";
    private static final String COVERAGE_DOMAIN = "NEREUS_V2_M5_PULSAR_COVERAGE_V2";

    public M5PulsarCoverageV2 {
        Objects.requireNonNull(binding, "binding");
        requireDigest(chainAuthorityRootSha256);
        requireDigest(chainSha256);
        requireCount(chain.size(), MAX_LEDGERS);
        requireCount(ranges.size(), MAX_RANGES);
        chain = List.copyOf(chain);
        ranges = List.copyOf(ranges);
        validateChain(chain);
        if (!chainSha256.equals(calculateChainSha256(binding, chainAuthorityRootSha256, chain))) {
            throw new IllegalArgumentException("Pulsar chain directory identity differs");
        }
        validateRanges(chain, ranges);
        countEntries(ranges);
    }

    public record Position(long ledgerId, long entryId) {
        public Position {
            if (ledgerId < 0 || entryId < 0) {
                throw new IllegalArgumentException("Pulsar entry position is negative");
            }
        }
    }

    public record ChainLedger(
            long ledgerId, boolean active, long entryEndExclusive, Sha256Digest sourceIdentitySha256) {
        public ChainLedger {
            if (ledgerId < 0 || entryEndExclusive < 0) {
                throw new IllegalArgumentException("Pulsar chain ledger boundary is negative");
            }
            requireDigest(sourceIdentitySha256);
        }
    }

    public record EntryRange(long ledgerId, long startEntryInclusive, long endEntryExclusive) {
        public EntryRange {
            if (ledgerId < 0 || startEntryInclusive < 0 || endEntryExclusive <= startEntryInclusive) {
                throw new IllegalArgumentException("Pulsar ledger entry range is empty or reversed");
            }
        }
    }

    public static M5PulsarCoverageV2 of(
            BindingIdentity binding, Sha256Digest authorityRoot, List<ChainLedger> chain, List<EntryRange> ranges) {
        requireCount(chain.size(), MAX_LEDGERS);
        return new M5PulsarCoverageV2(
                binding, authorityRoot, calculateChainSha256(binding, authorityRoot, chain), chain, ranges);
    }

    public long entryCount() {
        return countEntries(ranges);
    }

    /** Comparison uses this captured directory; Position deliberately has no global Comparable implementation. */
    public int compare(Position left, Position right) {
        int leftLedger = ordinal(chain, left.ledgerId());
        int rightLedger = ordinal(chain, right.ledgerId());
        requirePositionBoundary(left, chain.get(leftLedger));
        requirePositionBoundary(right, chain.get(rightLedger));
        return leftLedger == rightLedger
                ? Long.compare(left.entryId(), right.entryId())
                : Integer.compare(leftLedger, rightLedger);
    }

    public Optional<Position> firstAtOrAfter(Position requested) {
        int requestedLedger = ordinal(chain, requested.ledgerId());
        requirePositionBoundary(requested, chain.get(requestedLedger));
        for (EntryRange range : ranges) {
            int rangeLedger = ordinal(chain, range.ledgerId());
            if (rangeLedger > requestedLedger) {
                return Optional.of(new Position(range.ledgerId(), range.startEntryInclusive()));
            }
            if (rangeLedger == requestedLedger && requested.entryId() < range.endEntryExclusive()) {
                return Optional.of(
                        new Position(range.ledgerId(), Math.max(requested.entryId(), range.startEntryInclusive())));
            }
        }
        return Optional.empty();
    }

    public boolean contains(M5PulsarCoverageV2 other) {
        if (!sameChain(other)) {
            return false;
        }
        return other.ranges.stream().allMatch(wanted -> ranges.stream()
                .anyMatch(actual -> actual.ledgerId() == wanted.ledgerId()
                        && actual.startEntryInclusive() <= wanted.startEntryInclusive()
                        && actual.endEntryExclusive() >= wanted.endEntryExclusive()));
    }

    public boolean adjacentTo(M5PulsarCoverageV2 other) {
        if (!sameChain(other)) {
            return false;
        }
        return adjacent(chain, ranges.get(ranges.size() - 1), other.ranges.get(0));
    }

    boolean sameChain(M5PulsarCoverageV2 other) {
        return binding.equals(other.binding)
                && chainAuthorityRootSha256.equals(other.chainAuthorityRootSha256)
                && chainSha256.equals(other.chainSha256)
                && chain.equals(other.chain);
    }

    public M5PulsarCoverageV2 withRanges(List<EntryRange> selected) {
        return new M5PulsarCoverageV2(binding, chainAuthorityRootSha256, chainSha256, chain, selected);
    }

    Position firstPosition() {
        var first = ranges.get(0);
        return new Position(first.ledgerId(), first.startEntryInclusive());
    }

    Position endPosition() {
        var last = ranges.get(ranges.size() - 1);
        return new Position(last.ledgerId(), last.endEntryExclusive());
    }

    void writeRangeReference(DataOutputStream out) throws IOException {
        out.writeInt(ranges.size());
        for (EntryRange range : ranges) {
            out.writeLong(range.ledgerId());
            out.writeLong(range.startEntryInclusive());
            out.writeLong(range.endEntryExclusive());
        }
    }

    static M5PulsarCoverageV2 readRangeReference(DataInputStream in, M5PulsarCoverageV2 context) throws IOException {
        int count = in.readInt();
        requireCount(count, MAX_RANGES);
        var selected = new ArrayList<EntryRange>(count);
        for (int index = 0; index < count; index++) {
            selected.add(new EntryRange(in.readLong(), in.readLong(), in.readLong()));
        }
        return context.withRanges(selected);
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(VERSION);
                out.writeByte(DOMAIN_TAG);
                writeBinding(out, binding);
                writeDigest(out, chainAuthorityRootSha256);
                writeDigest(out, chainSha256);
                writeChain(out, chain);
                out.writeInt(ranges.size());
                for (EntryRange range : ranges) {
                    out.writeLong(range.ledgerId());
                    out.writeLong(range.startEntryInclusive());
                    out.writeLong(range.endEntryExclusive());
                }
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("Pulsar coverage exceeds its byte cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Pulsar coverage encoding failed", impossible);
        }
    }

    public Sha256Digest sha256() {
        return domainHash(COVERAGE_DOMAIN, encode());
    }

    public static M5PulsarCoverageV2 decode(CanonicalBytes bytes) {
        if (bytes == null || bytes.isEmpty() || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("Pulsar coverage length exceeds its byte cap");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != VERSION || in.readUnsignedByte() != DOMAIN_TAG) {
                throw new IllegalArgumentException("Pulsar coverage magic/version/domain differs");
            }
            var binding = new BindingIdentity(new TopicBindingId(readDigest(in)), readDigest(in), readDigest(in));
            var authority = readDigest(in);
            var chainSha = readDigest(in);
            int ledgers = in.readInt();
            requireCount(ledgers, MAX_LEDGERS);
            var chain = new ArrayList<ChainLedger>(ledgers);
            for (int index = 0; index < ledgers; index++) {
                long id = in.readLong();
                int active = in.readUnsignedByte();
                if (active > 1) {
                    throw new IllegalArgumentException("Pulsar ledger state is unknown");
                }
                chain.add(new ChainLedger(id, active == 1, in.readLong(), readDigest(in)));
            }
            int count = in.readInt();
            requireCount(count, MAX_RANGES);
            var ranges = new ArrayList<EntryRange>(count);
            for (int index = 0; index < count; index++) {
                ranges.add(new EntryRange(in.readLong(), in.readLong(), in.readLong()));
            }
            if (in.read() != -1) {
                throw new IllegalArgumentException("Pulsar coverage has trailing bytes");
            }
            var result = new M5PulsarCoverageV2(binding, authority, chainSha, chain, ranges);
            if (!result.encode().equals(bytes)) {
                throw new IllegalArgumentException("Pulsar coverage is not canonical");
            }
            return result;
        } catch (IOException invalid) {
            throw new IllegalArgumentException("Pulsar coverage is truncated or invalid", invalid);
        }
    }

    private static Sha256Digest calculateChainSha256(
            BindingIdentity binding, Sha256Digest authority, List<ChainLedger> chain) {
        requireCount(chain.size(), MAX_LEDGERS);
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                writeBinding(out, binding);
                writeDigest(out, authority);
                writeChain(out, chain);
            }
            return domainHash(CHAIN_DOMAIN, CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Pulsar chain encoding failed", impossible);
        }
    }

    private static Sha256Digest domainHash(String domain, CanonicalBytes body) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                byte[] ascii = domain.getBytes(StandardCharsets.US_ASCII);
                out.writeInt(ascii.length);
                out.write(ascii);
                out.write(body.toByteArray());
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Pulsar identity encoding failed", impossible);
        }
    }

    private static void writeChain(DataOutputStream out, List<ChainLedger> chain) throws IOException {
        out.writeInt(chain.size());
        for (ChainLedger ledger : chain) {
            out.writeLong(ledger.ledgerId());
            out.writeByte(ledger.active() ? 1 : 0);
            out.writeLong(ledger.entryEndExclusive());
            writeDigest(out, ledger.sourceIdentitySha256());
        }
    }

    private static void writeBinding(DataOutputStream out, BindingIdentity binding) throws IOException {
        writeDigest(out, binding.bindingId().digest());
        writeDigest(out, binding.incarnationSha256());
        writeDigest(out, binding.storageEpochSha256());
    }

    private static void writeDigest(DataOutputStream out, Sha256Digest digest) throws IOException {
        out.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream in) throws IOException {
        var bytes = new byte[32];
        in.readFully(bytes);
        return Sha256Digest.copyOf(bytes);
    }

    private static void validateChain(List<ChainLedger> chain) {
        var ids = new HashSet<Long>();
        for (int index = 0; index < chain.size(); index++) {
            var ledger = chain.get(index);
            if (!ids.add(ledger.ledgerId()) || ledger.active() && index != chain.size() - 1) {
                throw new IllegalArgumentException("Pulsar chain has duplicate ledgers or an interior active tail");
            }
        }
    }

    private static void validateRanges(List<ChainLedger> chain, List<EntryRange> ranges) {
        for (int index = 0; index < ranges.size(); index++) {
            var range = ranges.get(index);
            var ledger = chain.get(ordinal(chain, range.ledgerId()));
            if (ledger.active() || range.endEntryExclusive() > ledger.entryEndExclusive()) {
                throw new IllegalArgumentException("Pulsar range includes active or unsealed entries");
            }
            if (index > 0 && !canonicalSuccessor(chain, ranges.get(index - 1), range)) {
                throw new IllegalArgumentException("Pulsar ranges are not a canonical contiguous sealed cut");
            }
        }
    }

    private static boolean canonicalSuccessor(List<ChainLedger> chain, EntryRange previous, EntryRange next) {
        return previous.ledgerId() != next.ledgerId() && adjacent(chain, previous, next);
    }

    private static boolean adjacent(List<ChainLedger> chain, EntryRange previous, EntryRange next) {
        if (previous.ledgerId() == next.ledgerId()) {
            return previous.endEntryExclusive() == next.startEntryInclusive();
        }
        int before = ordinal(chain, previous.ledgerId());
        int after = ordinal(chain, next.ledgerId());
        if (after <= before
                || previous.endEntryExclusive() != chain.get(before).entryEndExclusive()
                || next.startEntryInclusive() != 0) {
            return false;
        }
        for (int index = before + 1; index < after; index++) {
            if (chain.get(index).active() || chain.get(index).entryEndExclusive() != 0) {
                return false;
            }
        }
        return true;
    }

    private static int ordinal(List<ChainLedger> chain, long ledgerId) {
        for (int index = 0; index < chain.size(); index++) {
            if (chain.get(index).ledgerId() == ledgerId) {
                return index;
            }
        }
        throw new IllegalArgumentException("Pulsar ledger is outside the captured chain");
    }

    private static void requirePositionBoundary(Position position, ChainLedger ledger) {
        if (position.entryId() > ledger.entryEndExclusive()) {
            throw new IllegalArgumentException("Pulsar position exceeds the captured ledger boundary");
        }
    }

    private static long countEntries(List<EntryRange> ranges) {
        long count = 0;
        for (EntryRange range : ranges) {
            count = Math.addExact(count, Math.subtractExact(range.endEntryExclusive(), range.startEntryInclusive()));
        }
        return count;
    }

    private static void requireCount(int count, int cap) {
        if (count <= 0 || count > cap) {
            throw new IllegalArgumentException("Pulsar coverage count exceeds its cap");
        }
    }

    private static void requireDigest(Sha256Digest digest) {
        if (Objects.requireNonNull(digest, "digest").isZero()) {
            throw new IllegalArgumentException("Pulsar coverage identity is zero");
        }
    }
}
