/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static com.nereusstream.metadata.oxia.F4MetadataTestValues.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PhysicalObjectReferencePaginationScaleTest {
    private static final String BASE32 = "abcdefghijklmnopqrstuvwxyz234567";
    private static final int REFERENCE_COUNT = 1_000;
    private static final int PAGE_SIZE = 127;
    private static final int EXPECTED_PAGE_COUNT = 8;

    @Test
    void scansOneThousandReaderLeasesAndProtectionsWithoutTruncationAndRestarts() {
        try (OxiaJavaPhysicalObjectMetadataStore store = store()) {
            VersionedPhysicalObjectRoot root = store.createRoot(
                            CLUSTER,
                            F4MetadataTestValues.physicalRoot(
                                    PhysicalObjectLifecycle.ACTIVE))
                    .join();
            ObjectKeyHash object = new ObjectKeyHash(
                    root.value().objectKeyHash());
            List<String> expectedReaderKeys = new ArrayList<>(REFERENCE_COUNT);
            List<String> expectedProtectionKeys = new ArrayList<>(REFERENCE_COUNT);

            for (int index = 0; index < REFERENCE_COUNT; index++) {
                expectedReaderKeys.add(store.createOrCompareReaderLease(
                                CLUSTER, readerLease(object, index))
                        .join()
                        .key());
                expectedProtectionKeys.add(store.createProtection(
                                CLUSTER, protection(object, index))
                        .join()
                        .key());
            }
            expectedReaderKeys.sort(Comparator.naturalOrder());
            expectedProtectionKeys.sort(Comparator.naturalOrder());

            assertThat(scanReaderKeys(store, object))
                    .containsExactlyElementsOf(expectedReaderKeys);
            assertThat(scanProtectionKeys(store, object))
                    .containsExactlyElementsOf(expectedProtectionKeys);

            // A fresh pass must restart only from the empty continuation and
            // reproduce the exact same complete inventories.
            assertThat(scanReaderKeys(store, object))
                    .containsExactlyElementsOf(expectedReaderKeys);
            assertThat(scanProtectionKeys(store, object))
                    .containsExactlyElementsOf(expectedProtectionKeys);
        }
    }

    private static List<String> scanReaderKeys(
            PhysicalObjectMetadataStore store, ObjectKeyHash object) {
        List<String> keys = new ArrayList<>(REFERENCE_COUNT);
        Optional<F4ScanToken> continuation = Optional.empty();
        int pages = 0;
        do {
            ReaderLeaseScanPage page = store.scanReaderLeases(
                            CLUSTER, object, continuation, PAGE_SIZE)
                    .join();
            pages++;
            assertPage(page.values().stream()
                    .map(VersionedReaderLease::key)
                    .toList(), page.continuation(), keys);
            keys.addAll(page.values().stream()
                    .map(VersionedReaderLease::key)
                    .toList());
            continuation = page.continuation();
        } while (continuation.isPresent());
        assertThat(pages).isEqualTo(EXPECTED_PAGE_COUNT);
        assertThat(keys).hasSize(REFERENCE_COUNT).doesNotHaveDuplicates();
        return keys;
    }

    private static List<String> scanProtectionKeys(
            PhysicalObjectMetadataStore store, ObjectKeyHash object) {
        List<String> keys = new ArrayList<>(REFERENCE_COUNT);
        Optional<F4ScanToken> continuation = Optional.empty();
        int pages = 0;
        do {
            ObjectProtectionScanPage page = store.scanProtections(
                            CLUSTER, object, continuation, PAGE_SIZE)
                    .join();
            pages++;
            assertPage(page.values().stream()
                    .map(VersionedObjectProtection::key)
                    .toList(), page.continuation(), keys);
            keys.addAll(page.values().stream()
                    .map(VersionedObjectProtection::key)
                    .toList());
            continuation = page.continuation();
        } while (continuation.isPresent());
        assertThat(pages).isEqualTo(EXPECTED_PAGE_COUNT);
        assertThat(keys).hasSize(REFERENCE_COUNT).doesNotHaveDuplicates();
        return keys;
    }

    private static void assertPage(
            List<String> pageKeys,
            Optional<F4ScanToken> continuation,
            List<String> precedingKeys) {
        assertThat(pageKeys).isNotEmpty().hasSizeLessThanOrEqualTo(PAGE_SIZE).isSorted();
        if (continuation.isPresent()) {
            assertThat(pageKeys).hasSize(PAGE_SIZE);
        }
        if (!precedingKeys.isEmpty()) {
            assertThat(pageKeys.get(0))
                    .isGreaterThan(precedingKeys.get(precedingKeys.size() - 1));
        }
    }

    private static ObjectReaderLeaseRecord readerLease(
            ObjectKeyHash object, int index) {
        return new ObjectReaderLeaseRecord(
                1,
                object.value(),
                base32Id('d', index + 1),
                base32Id('e', index + 1),
                1,
                100,
                10_000,
                9_000,
                index,
                0);
    }

    private static ObjectProtectionRecord protection(
            ObjectKeyHash object, int index) {
        String identity = String.format("scale-reference-%04d", index);
        return new ObjectProtectionRecord(
                1,
                object.value(),
                ObjectProtectionType.MATERIALIZATION_SOURCE.wireId(),
                identity,
                "/owners/f4/scale/" + identity,
                index + 1L,
                F4MetadataTestValues.HASH_B,
                1,
                100,
                0,
                0);
    }

    private static String base32Id(char fill, int value) {
        char[] result = "a".repeat(26).toCharArray();
        result[0] = fill;
        int remaining = value;
        for (int index = result.length - 1; remaining != 0 && index > 0; index--) {
            result[index] = BASE32.charAt(remaining & 31);
            remaining >>>= 5;
        }
        if (remaining != 0) {
            throw new IllegalArgumentException("scale identifier exceeds fixture capacity");
        }
        return new String(result);
    }

    private static OxiaJavaPhysicalObjectMetadataStore store() {
        return new OxiaJavaPhysicalObjectMetadataStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    }
}
