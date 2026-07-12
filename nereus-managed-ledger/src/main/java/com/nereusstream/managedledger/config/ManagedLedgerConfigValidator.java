/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.config;

import com.nereusstream.api.ApiLimits;
import com.nereusstream.api.MetadataCanonicalizer;
import java.util.Map;
import java.util.Objects;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;

/** Captures one immutable F2 operation view from the mutable stock configuration object. */
public final class ManagedLedgerConfigValidator {
    private ManagedLedgerConfigValidator() {
    }

    public static ManagedLedgerConfigView captureForOperation(ManagedLedgerConfig source) {
        ManagedLedgerConfig config = Objects.requireNonNull(source, "source");
        String storageClass = config.getStorageClassName();
        if (storageClass != null && !"nereus".equals(storageClass)) {
            throw new IllegalArgumentException("managed-ledger storage class must be null or nereus");
        }
        if (config.getRetentionTimeMillis() != 0 || config.getRetentionSizeInMB() != 0) {
            throw new IllegalArgumentException("F2 does not support managed-ledger retention policies");
        }
        if (config.getManagedLedgerInterceptor() != null) {
            throw new IllegalArgumentException("F2 does not support ManagedLedgerInterceptor");
        }
        if (config.getLedgerOffloader() != NullLedgerOffloader.INSTANCE
                || config.isTriggerOffloadOnTopicLoad()) {
            throw new IllegalArgumentException("F2 does not support Pulsar ledger offload");
        }
        if (config.getShadowSource() != null || config.getShadowSourceName() != null) {
            throw new IllegalArgumentException("F2 does not support shadow managed ledgers");
        }
        if (config.isAutoSkipNonRecoverableData()) {
            throw new IllegalArgumentException("F2 never auto-skips non-recoverable data");
        }
        return new ManagedLedgerConfigView(storageClass, config.getThrottleMarkDelete());
    }

    public static ManagedLedgerOpenConfigView captureForOpen(ManagedLedgerConfig source) {
        ManagedLedgerConfig config = Objects.requireNonNull(source, "source");
        ManagedLedgerConfigView operation = captureForOperation(config);
        Map<String, String> properties = MetadataCanonicalizer.canonicalStringMap(
                config.getProperties() == null ? Map.of() : config.getProperties(),
                ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES,
                "managedLedgerProperties");
        return new ManagedLedgerOpenConfigView(operation, config.isCreateIfMissing(), properties);
    }
}
