/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.BookKeeperMetadataTestValues;
import com.nereusstream.metadata.oxia.records.AllocationSlotLifecycle;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BookKeeperMetadataCodecContractTest {
    @Test
    void sevenV1RecordsRoundTripThroughRegistryAndFactory() {
        Map<String, String> golden = Map.of(
                "writer", "3484878b48ef882e76a3eace933c36764d1e78dd8a72af9731e46ec85bf81ff4",
                "allocation", "fd89803e4242f9559fe176c3273609efda1ef86556d81837bc9f560940310175",
                "slot", "268a980a3ed4c3b199facb40c7b3b7aa1263a0e31c793bb1bb3871c97047540d",
                "root", "f0b10758d85f15a60c63d54cd5c2e38fc4310028e9f0db01e7c8397ccedeb925",
                "reservation", "b61c95878d3038c813630defa33feb94126c824a95dbd9c0b3665f0e563a8a74",
                "protection", "13dbd7870b4674202e5af8c9b4d16f1572c8c66312401d121d3b8101511bcded",
                "reader", "19b75b7a0b2657130cb0061ed0c4efe79a2a9909b17908723e0adc43934c8f7c");
        for (Sample sample : samples()) {
            byte[] encoded = encode(sample);
            assertThat(decode(sample, encoded)).as(sample.name()).isEqualTo(sample.value());
            assertThat(factoryDecode(sample, encoded)).as(sample.name()).isEqualTo(sample.value());
            assertThat(sha256(encoded)).as(sample.name() + " frozen envelope SHA-256")
                    .isEqualTo(golden.get(sample.name()));
        }
    }

    @Test
    void everyCodecRejectsTruncationTrailingBytesAndEnvelopeCorruption() {
        for (Sample sample : samples()) {
            byte[] encoded = encode(sample);
            assertThatThrownBy(() -> decode(sample, Arrays.copyOf(encoded, encoded.length - 1)))
                    .as(sample.name() + " truncated")
                    .isInstanceOf(MetadataCodecException.class);
            byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
            assertThatThrownBy(() -> decode(sample, trailing))
                    .as(sample.name() + " trailing")
                    .isInstanceOf(MetadataCodecException.class);
            byte[] corrupt = encoded.clone();
            corrupt[corrupt.length - 1] ^= 1;
            assertThatThrownBy(() -> decode(sample, corrupt))
                    .as(sample.name() + " checksum")
                    .isInstanceOf(MetadataCodecException.class);
        }
    }

    @Test
    void explicitWireIdsRejectUnknownValues() {
        assertThatThrownBy(() -> BookKeeperWriterLifecycle.fromWireId(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LedgerAllocationLifecycle.fromWireId(8)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllocationSlotLifecycle.fromWireId(4)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BookKeeperLedgerLifecycle.fromWireId(10)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AppendReservationLifecycle.fromWireId(7)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BookKeeperProtectionType.fromWireId(6)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ProtectionLifecycle.fromWireId(3)).isInstanceOf(IllegalArgumentException.class);
    }

    private static List<Sample> samples() {
        return List.of(
                new Sample("writer", BookKeeperMetadataTestValues.writer(), BookKeeperWriterStateRecord.class),
                new Sample("allocation", BookKeeperMetadataTestValues.allocation(), LedgerAllocationIntentRecord.class),
                new Sample("slot", BookKeeperMetadataTestValues.slot(), BookKeeperAllocationSlotRecord.class),
                new Sample("root", BookKeeperMetadataTestValues.root(), BookKeeperLedgerRootRecord.class),
                new Sample("reservation", BookKeeperMetadataTestValues.reservation(), BookKeeperAppendReservationRecord.class),
                new Sample("protection", BookKeeperMetadataTestValues.protection(), BookKeeperLedgerProtectionRecord.class),
                new Sample("reader", BookKeeperMetadataTestValues.readerLease(), BookKeeperLedgerReaderLeaseRecord.class));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static byte[] encode(Sample sample) {
        return BookKeeperMetadataCodecs.encodeEnvelope(sample.value(), (Class) sample.type());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object decode(Sample sample, byte[] encoded) {
        return BookKeeperMetadataCodecs.decodeEnvelope(encoded, (Class) sample.type());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object factoryDecode(Sample sample, byte[] encoded) {
        return MetadataRecordCodecFactory.decodeEnvelope(encoded, (Class) sample.type());
    }

    private static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (Exception impossible) { throw new IllegalStateException(impossible); }
    }

    private record Sample(String name, Object value, Class<?> type) { }
}
