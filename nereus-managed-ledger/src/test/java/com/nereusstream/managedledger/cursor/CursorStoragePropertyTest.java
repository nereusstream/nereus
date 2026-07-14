/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CursorStoragePropertyTest {
    @Test
    void durablePropertyMutationsPreserveInternalKeysAndProveResponseLoss() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = context.storage.open(
                            owner,
                            "subscription-a",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Earliest(),
                                    Map.of("initial-position", 1L),
                                    Map.of(
                                            "external-old", "old",
                                            "#pulsar.internal.keep", "internal"),
                                    0,
                                    20))
                    .join();

            CursorState replaced = context.storage.mutateCursorProperties(
                            handle,
                            new CursorPropertyMutation.ReplaceExternal(
                                    Map.of("external-new", "new")))
                    .join()
                    .state();
            assertThat(replaced.cursorProperties())
                    .containsExactlyInAnyOrderEntriesOf(Map.of(
                            "external-new", "new",
                            "#pulsar.internal.keep", "internal"));

            context.storage.mutateCursorProperties(
                            handle,
                            new CursorPropertyMutation.Put("transient", "value"))
                    .join();
            CursorState removed = context.storage.mutateCursorProperties(
                            handle,
                            new CursorPropertyMutation.Remove("transient"))
                    .join()
                    .state();
            assertThat(removed.cursorProperties()).doesNotContainKey("transient");

            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_CURSOR,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    new NereusException(
                            ErrorCode.METADATA_UNAVAILABLE,
                            true,
                            "position-property response lost"));
            CursorMutationResult flushed = context.storage.flushPositionProperties(
                            handle, Map.of("stage", 9L))
                    .join();
            assertThat(flushed.outcome()).isEqualTo(CursorMutationOutcome.ALREADY_APPLIED);
            assertThat(flushed.state().positionProperties())
                    .containsExactly(Map.entry("stage", 9L));

            CursorState durable = context.storage
                    .claimAndLoadActiveCursors(owner)
                    .join()
                    .getFirst()
                    .state();
            assertThat(durable.cursorProperties()).isEqualTo(removed.cursorProperties());
            assertThat(durable.positionProperties()).containsExactly(Map.entry("stage", 9L));
        }
    }
}
