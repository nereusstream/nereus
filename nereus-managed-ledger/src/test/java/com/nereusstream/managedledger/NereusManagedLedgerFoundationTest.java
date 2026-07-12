/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.managedledger.config.ManagedLedgerConfigValidator;
import com.nereusstream.managedledger.config.ManagedLedgerOpenConfigView;
import com.nereusstream.managedledger.projection.VirtualLedgerProjection;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.junit.jupiter.api.Test;

class NereusManagedLedgerFoundationTest {
    private static final String NAME = "tenant/ns/persistent/topic";

    @Test
    void factoryConfigEnforcesClosedF2LimitsAndCloseBudget() {
        NereusManagedLedgerFactoryConfig defaults = NereusManagedLedgerFactoryConfig.defaults(1024);
        assertThat(defaults.storageClassName()).isEqualTo("nereus");
        assertThat(defaults.closeTimeout()).isGreaterThanOrEqualTo(
                defaults.appendTimeout().plus(defaults.appendRecoveryTimeout()));

        assertThatThrownBy(() -> config("bookkeeper", Duration.ofSeconds(75), 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> config("nereus", Duration.ofSeconds(59), 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cover");
        assertThatThrownBy(() -> config("nereus", Duration.ofSeconds(75), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void managedLedgerConfigCaptureIsImmutableAndFailsUnsupportedFeatures() {
        LinkedHashMap<String, String> properties = new LinkedHashMap<>();
        properties.put("z", "2");
        properties.put("a", "1");
        ManagedLedgerConfig valid = new ManagedLedgerConfig();
        valid.setStorageClassName("nereus");
        valid.setThrottleMarkDelete(2.5);
        valid.setProperties(properties);

        ManagedLedgerOpenConfigView captured = ManagedLedgerConfigValidator.captureForOpen(valid);
        properties.put("later", "mutation");

        assertThat(captured.operationView().storageClassName()).isEqualTo("nereus");
        assertThat(captured.operationView().throttleMarkDelete()).isEqualTo(2.5);
        assertThat(captured.initialProperties()).containsExactly(
                Map.entry("a", "1"), Map.entry("z", "2"));
        assertThatThrownBy(() -> captured.initialProperties().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);

        ManagedLedgerConfig retention = new ManagedLedgerConfig();
        retention.setRetentionTime(1, TimeUnit.SECONDS);
        assertRejected(retention, "retention");
        ManagedLedgerConfig storageClass = new ManagedLedgerConfig();
        storageClass.setStorageClassName("bookkeeper");
        assertRejected(storageClass, "storage class");
        ManagedLedgerConfig shadow = new ManagedLedgerConfig();
        shadow.setProperties(Map.of("PULSAR.SHADOW_SOURCE", "source"));
        assertRejected(shadow, "shadow");
        ManagedLedgerConfig autoSkip = new ManagedLedgerConfig();
        autoSkip.setAutoSkipNonRecoverableData(true);
        assertRejected(autoSkip, "auto-skips");
        ManagedLedgerConfig offload = new ManagedLedgerConfig();
        offload.setTriggerOffloadOnTopicLoad(true);
        assertRejected(offload, "offload");
    }

    @Test
    void durableInspectionSnapshotRequiresBothExactRecords() {
        VirtualLedgerProjection projection = projection();
        StreamMetadata active = metadata(StreamState.ACTIVE);

        assertThat(NereusStorageStateSnapshot.missing().state()).isEqualTo(NereusDurableStorageState.MISSING);
        NereusStorageStateSnapshot present = new NereusStorageStateSnapshot(
                NereusDurableStorageState.ACTIVE, Optional.of(projection), Optional.of(active));
        assertThat(present.streamMetadata()).contains(active);
        assertThatThrownBy(() -> new NereusStorageStateSnapshot(
                        NereusDurableStorageState.MISSING, Optional.of(projection), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NereusStorageStateSnapshot(
                        NereusDurableStorageState.SEALED, Optional.of(projection), Optional.of(active)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NereusStorageStateSnapshot(
                        NereusDurableStorageState.ACTIVE,
                        Optional.of(projection),
                        Optional.of(new StreamMetadata(
                                ManagedLedgerProjectionNames.streamId(NAME, 2),
                                ManagedLedgerProjectionNames.streamName(NAME, 2),
                                StreamState.ACTIVE,
                                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                                Map.of(), 1, 1, 0, 0, 0))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writeFenceSnapshotRejectsGenerationZero() {
        AppendAttemptId attemptId = new AppendAttemptId("run/1");
        assertThat(new NereusWriteFenceSnapshot(1, attemptId).attemptId()).isEqualTo(attemptId);
        assertThatThrownBy(() -> new NereusWriteFenceSnapshot(0, attemptId))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static NereusManagedLedgerFactoryConfig config(
            String storageClass,
            Duration close,
            int limit) {
        return new NereusManagedLedgerFactoryConfig(
                storageClass,
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                close,
                Duration.ofSeconds(1),
                limit,
                limit,
                limit,
                limit,
                limit,
                limit);
    }

    private static void assertRejected(ManagedLedgerConfig config, String message) {
        assertThatThrownBy(() -> ManagedLedgerConfigValidator.captureForOperation(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    private static VirtualLedgerProjection projection() {
        return new VirtualLedgerProjection(
                ManagedLedgerProjectionNames.streamId(NAME, 1),
                NAME,
                3,
                1,
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID,
                1,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1,
                1,
                1);
    }

    private static StreamMetadata metadata(StreamState state) {
        return new StreamMetadata(
                ManagedLedgerProjectionNames.streamId(NAME, 1),
                ManagedLedgerProjectionNames.streamName(NAME, 1),
                state,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                Map.of(),
                1,
                1,
                0,
                0,
                0);
    }
}
