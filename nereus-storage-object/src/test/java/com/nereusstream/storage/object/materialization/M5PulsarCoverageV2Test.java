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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.ChainLedger;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** Codec/declared-value checks only. These fixtures never qualify an actual Native source or Ledger Chain. */
class M5PulsarCoverageV2Test {
    @Test
    void matchesIndependentWireGoldenAndIdentityDomains() throws Exception {
        var expected = golden();
        var value = coverage(List.of(new EntryRange(9, 1, 2), new EntryRange(3, 0, 2)));
        assertThat(value.encode()).isEqualTo(expected);
        assertThat(M5PulsarCoverageV2.decode(expected)).isEqualTo(value);
        assertThat(expected.length()).isEqualTo(372);
        assertThat(Sha256Digest.hash(expected).toHex())
                .isEqualTo("139686fdb9bc250d640e598a074f01957a2ce9e25a025aab8a9bcb692eb2921f");
        assertThat(value.chainSha256().toHex())
                .isEqualTo("b0bd157c6322d5dc19a0b830a806bb7d18a1ad2ba27c0216b3f648f108d47903");
        assertThat(value.sha256().toHex())
                .isEqualTo("1b2eefe54c97ca05dbd67521858546d4037b99d909662668d13af389f6c3b1e8");
    }

    @Test
    void ordersSameEntryIdsByCapturedChainAndFindsCrossLedgerSuccessor() {
        var value = coverage(List.of(new EntryRange(9, 1, 2), new EntryRange(3, 0, 2)));
        assertThat(value.compare(new Position(9, 0), new Position(3, 0))).isNegative();
        assertThat(value.compare(new Position(3, 1), new Position(3, 0))).isPositive();
        assertThat(value.firstAtOrAfter(new Position(9, 0))).contains(new Position(9, 1));
        assertThat(value.firstAtOrAfter(new Position(9, 2))).contains(new Position(3, 0));
        assertThat(value.firstAtOrAfter(new Position(3, 1))).contains(new Position(3, 1));
        assertThat(value.firstAtOrAfter(new Position(3, 2))).isEmpty();
        assertThat(value.firstAtOrAfter(new Position(7, 0))).isEmpty();
        assertThat(value.entryCount()).isEqualTo(3);
        assertThatThrownBy(() -> value.firstAtOrAfter(new Position(8, 0)))
                .hasMessageContaining("outside the captured chain");
        assertThatThrownBy(() -> value.compare(new Position(9, 3), new Position(3, 0)))
                .hasMessageContaining("captured ledger boundary");
    }

    @Test
    void scopesContainmentAndAdjacencyToExactBindingAndChain() {
        var left = coverage(List.of(new EntryRange(9, 1, 2)));
        var right = coverage(List.of(new EntryRange(3, 0, 2)));
        var whole = coverage(List.of(new EntryRange(9, 1, 2), new EntryRange(3, 0, 2)));
        assertThat(left.adjacentTo(right)).isTrue();
        assertThat(right.adjacentTo(left)).isFalse();
        assertThat(whole.contains(left)).isTrue();
        assertThat(left.contains(whole)).isFalse();
        var foreign = new BindingIdentity(new TopicBindingId(digest("other")), digest("incarnation"), digest("epoch"));
        var anotherBinding = M5PulsarCoverageV2.of(foreign, digest("authority"), chain(), right.ranges());
        var anotherRoot = M5PulsarCoverageV2.of(binding(), digest("new-authority"), chain(), right.ranges());
        assertThat(left.adjacentTo(anotherBinding)).isFalse();
        assertThat(left.adjacentTo(anotherRoot)).isFalse();
        assertThat(whole.contains(anotherBinding)).isFalse();
        assertThat(whole.contains(anotherRoot)).isFalse();
    }

    @Test
    void rejectsForeignActiveOverlappingAndUnexplainedMissingLedgerRanges() {
        for (var ranges : List.of(
                List.of(new EntryRange(8, 0, 1)),
                List.of(new EntryRange(7, 0, 1)),
                List.of(new EntryRange(9, 0, 3)),
                List.of(new EntryRange(9, 0, 1), new EntryRange(3, 0, 2)),
                List.of(new EntryRange(9, 0, 2), new EntryRange(3, 1, 2)),
                List.of(new EntryRange(3, 0, 2), new EntryRange(9, 0, 2)),
                List.of(new EntryRange(9, 0, 1), new EntryRange(9, 1, 2)))) {
            assertThatThrownBy(() -> coverage(ranges)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void onlyExplicitEmptySealedLedgerCanIntervene() {
        var withHole = List.of(
                new ChainLedger(9, false, 2, digest("sealed-9")),
                new ChainLedger(5, false, 1, digest("sealed-5")),
                new ChainLedger(3, false, 2, digest("sealed-3")));
        var cut = List.of(new EntryRange(9, 1, 2), new EntryRange(3, 0, 2));
        assertThatThrownBy(() -> M5PulsarCoverageV2.of(binding(), digest("authority"), withHole, cut))
                .hasMessageContaining("contiguous sealed cut");
        var withEmpty = List.of(withHole.get(0), new ChainLedger(5, false, 0, digest("empty-5")), withHole.get(2));
        assertThat(M5PulsarCoverageV2.of(binding(), digest("authority"), withEmpty, cut)
                        .entryCount())
                .isEqualTo(3);
    }

    @Test
    void rejectsDuplicateDirectoryAndInteriorActiveLedger() {
        for (var directory : List.of(
                List.of(chain().get(0), chain().get(0)),
                List.of(chain().get(2), chain().get(0)),
                List.of(chain().get(2), new ChainLedger(8, true, 1, digest("active-8"))))) {
            assertThatThrownBy(() -> M5PulsarCoverageV2.of(
                            binding(), digest("authority"), directory, List.of(new EntryRange(9, 0, 1))))
                    .hasMessageContaining("duplicate ledgers or an interior active tail");
        }
    }

    @Test
    void rejectsAllTruncationsUnknownTagsTrailingBytesAndChangedDirectoryRoot() throws Exception {
        byte[] encoded = golden().toByteArray();
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            assertThatThrownBy(() -> M5PulsarCoverageV2.decode(CanonicalBytes.copyOf(truncated)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (int offset : new int[] {0, 7, 8, 137, 181}) {
            byte[] invalid = encoded.clone();
            invalid[offset] = (byte) 0xff;
            assertThatThrownBy(() -> M5PulsarCoverageV2.decode(CanonicalBytes.copyOf(invalid)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() ->
                        M5PulsarCoverageV2.decode(CanonicalBytes.copyOf(Arrays.copyOf(encoded, encoded.length + 1))))
                .hasMessageContaining("trailing bytes");
        var value = coverage(List.of(new EntryRange(9, 1, 2), new EntryRange(3, 0, 2)));
        assertThatThrownBy(() -> new M5PulsarCoverageV2(
                        value.binding(),
                        value.chainAuthorityRootSha256(),
                        value.chainSha256(),
                        List.of(chain().get(1), chain().get(0), chain().get(2)),
                        value.ranges()))
                .hasMessageContaining("directory identity differs");
    }

    @Test
    void rejectsCountsBeforeAllocationAndCheckedEntrySumOverflow() throws Exception {
        for (int count : new int[] {-1, 0, 257, Integer.MAX_VALUE}) {
            byte[] invalid = golden().toByteArray();
            ByteBuffer.wrap(invalid).putInt(169, count);
            assertThatThrownBy(() -> M5PulsarCoverageV2.decode(CanonicalBytes.copyOf(invalid)))
                    .hasMessageContaining("count exceeds its cap");
        }
        assertThatThrownBy(() ->
                        M5PulsarCoverageV2.decode(CanonicalBytes.copyOf(new byte[M5PulsarCoverageV2.MAX_BYTES + 1])))
                .hasMessageContaining("byte cap");
        var tooMany = IntStream.range(0, 257)
                .mapToObj(id -> new ChainLedger(id, false, 1, digest("source")))
                .toList();
        assertThatThrownBy(() -> M5PulsarCoverageV2.of(
                        binding(), digest("authority"), tooMany, List.of(new EntryRange(0, 0, 1))))
                .hasMessageContaining("count exceeds its cap");
        var huge = List.of(
                new ChainLedger(0, false, Long.MAX_VALUE, digest("zero")),
                new ChainLedger(1, false, Long.MAX_VALUE, digest("one")));
        assertThatThrownBy(() -> M5PulsarCoverageV2.of(
                        binding(),
                        digest("authority"),
                        huge,
                        List.of(new EntryRange(0, 0, Long.MAX_VALUE), new EntryRange(1, 0, Long.MAX_VALUE))))
                .isInstanceOf(ArithmeticException.class);
    }

    private static M5PulsarCoverageV2 coverage(List<EntryRange> ranges) {
        return M5PulsarCoverageV2.of(binding(), digest("authority"), chain(), ranges);
    }

    private static BindingIdentity binding() {
        return new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("epoch"));
    }

    private static List<ChainLedger> chain() {
        return List.of(
                new ChainLedger(9, false, 2, digest("sealed-9")),
                new ChainLedger(3, false, 2, digest("sealed-3")),
                new ChainLedger(7, true, 1, digest("active-7")));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }

    private static CanonicalBytes golden() throws IOException {
        try (var stream =
                M5PulsarCoverageV2Test.class.getResourceAsStream("/materialization/m5-pulsar-coverage-v2.hex")) {
            assertThat(stream).isNotNull();
            return CanonicalBytes.copyOf(HexFormat.of()
                    .parseHex(new String(stream.readAllBytes(), StandardCharsets.US_ASCII).replaceAll("\\s+", "")));
        }
    }
}
