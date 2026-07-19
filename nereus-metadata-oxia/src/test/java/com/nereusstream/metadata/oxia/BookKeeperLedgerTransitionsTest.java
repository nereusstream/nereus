/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import com.nereusstream.metadata.oxia.testing.BookKeeperMetadataStoreContractScenario;
import org.junit.jupiter.api.Test;

class BookKeeperLedgerTransitionsTest {
    private static final String CLUSTER = BookKeeperMetadataStoreContractScenario.CLUSTER;
    private static final String PROVIDER_SCOPE = BookKeeperMetadataStoreContractScenario.PROVIDER_SCOPE;
    private static final BookKeeperKeyspace KEYS = new BookKeeperMetadataStoreConfig(128, 4, 128, 256)
            .keyspace(CLUSTER);

    @Test
    void rejectsIllegalTransitionsAndImmutableIdentityDrift() {
        var prepared = BookKeeperMetadataStoreContractScenario.allocation(
                LedgerAllocationLifecycle.PREPARED, false, "", 120);
        var activated = BookKeeperMetadataStoreContractScenario.allocation(
                LedgerAllocationLifecycle.ACTIVATED,
                false,
                BookKeeperMetadataStoreContractScenario.CUSTOM_METADATA,
                121);
        assertThatThrownBy(() -> BookKeeperMetadataTransitions.allocation(prepared, activated))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("illegal allocation lifecycle");

        var writer = BookKeeperMetadataStoreContractScenario.activeWriter(3, 3, 300, 2);
        var regressedWriter = BookKeeperMetadataStoreContractScenario.activeWriter(4, 2, 299, 1);
        assertThatThrownBy(() -> BookKeeperMetadataTransitions.writer(writer, regressedWriter))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("counters moved backward");

        BookKeeperLedgerRootRecord active = BookKeeperMetadataStoreContractScenario.root(
                KEYS, 101, BookKeeperLedgerLifecycle.ACTIVE, 2);
        BookKeeperLedgerRootRecord drifted = root(
                active, active.streamId() + "-drifted", BookKeeperLedgerLifecycle.SEALING, 3, false);
        assertThatThrownBy(() -> BookKeeperMetadataTransitions.root(active, drifted))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identity drift");

        BookKeeperLedgerProtectionRecord protection =
                BookKeeperMetadataStoreContractScenario.protection(
                        KEYS.ledgerIdentitySha256(PROVIDER_SCOPE, 101), ProtectionLifecycle.ACTIVE);
        BookKeeperLedgerProtectionRecord retiredWithAnotherOwner = new BookKeeperLedgerProtectionRecord(
                protection.schemaVersion(),
                protection.ledgerIdentitySha256(),
                protection.clusterAlias(),
                protection.ledgerId(),
                protection.rootLifecycleEpoch(),
                protection.ledgerRangeSlot(),
                protection.protectionSlot(),
                protection.protectionTypeId(),
                protection.referenceId(),
                protection.firstEntryId(),
                protection.entryCount(),
                protection.rangeChecksumSha256(),
                protection.streamId(),
                protection.offsetStart(),
                protection.offsetEnd(),
                protection.commitVersion(),
                "/another/owner",
                protection.ownerMetadataVersion(),
                protection.ownerIdentitySha256(),
                ProtectionLifecycle.RETIRED,
                protection.createdAtMillis(),
                protection.expiresAtMillis(),
                0);
        assertThatThrownBy(() -> BookKeeperMetadataTransitions.protection(
                        protection, retiredWithAnotherOwner))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact historical owner");
    }

    @Test
    void rejectsInvalidLifecycleFieldsBeforePersistence() {
        assertThatThrownBy(() -> BookKeeperMetadataStoreContractScenario.allocation(
                        LedgerAllocationLifecycle.PREPARED, true, "", 120))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pre-transmission allocation");

        BookKeeperLedgerRootRecord active = BookKeeperMetadataStoreContractScenario.root(
                KEYS, 101, BookKeeperLedgerLifecycle.ACTIVE, 2);
        assertThatThrownBy(() -> root(
                        active, active.streamId(), BookKeeperLedgerLifecycle.MARKED, 3, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lateCreateHazard permanently vetoes physical GC");
    }

    private static BookKeeperLedgerRootRecord root(
            BookKeeperLedgerRootRecord before,
            String streamId,
            BookKeeperLedgerLifecycle lifecycle,
            long lifecycleEpoch,
            boolean lateCreateHazard) {
        boolean marked = lifecycle == BookKeeperLedgerLifecycle.MARKED;
        return new BookKeeperLedgerRootRecord(
                before.schemaVersion(),
                before.ledgerIdentitySha256(),
                before.clusterAlias(),
                before.providerScopeSha256(),
                before.ledgerId(),
                streamId,
                before.segmentSequence(),
                before.allocationId(),
                before.allocationSlot(),
                before.configurationBindingSha256(),
                before.ledgerIdNamespaceSha256(),
                lateCreateHazard,
                before.writerId(),
                before.writerRunIdHash(),
                before.appendSessionEpoch(),
                before.fencingTokenHash(),
                before.ensembleSize(),
                before.writeQuorumSize(),
                before.ackQuorumSize(),
                before.digestType(),
                before.customMetadataSha256(),
                lifecycle,
                lifecycleEpoch,
                before.createdAtMillis(),
                before.activatedAtMillis(),
                marked ? 120 : before.sealStartedAtMillis(),
                marked ? 130 : before.sealedAtMillis(),
                before.sealedLastEntryId(),
                before.sealedLength(),
                marked ? "sealed" : before.sealReason(),
                marked ? "gc-attempt" : before.gcAttemptId(),
                marked ? "f".repeat(64) : before.referenceSetSha256(),
                marked ? 140 : before.markedAtMillis(),
                marked ? 150 : before.deleteNotBeforeMillis(),
                before.deleteStartedAtMillis(),
                before.firstAbsentAtMillis(),
                before.deletedAtMillis(),
                before.stateReason(),
                0);
    }
}
