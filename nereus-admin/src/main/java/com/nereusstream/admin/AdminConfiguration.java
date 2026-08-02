/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerGcConfiguration;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.pulsar.NereusBookKeeperRuntimeConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

public final class AdminConfiguration {

    private static final String CLUSTER = "cluster";
    private static final String OXIA_SERVICE_ADDRESS = "oxia.serviceAddress";
    private static final String OXIA_NAMESPACE = "oxia.namespace";
    private static final String OXIA_SESSION_TIMEOUT_SECONDS = "oxia.sessionTimeoutSeconds";
    private static final String OXIA_MAX_PENDING_OPERATIONS = "oxia.maxPendingOperations";

    private static final String OBJECT_STORE_PROVIDER_CLASS = "objectStore.providerClassName";
    private static final String OBJECT_STORE_ENDPOINT = "objectStore.endpoint";
    private static final String OBJECT_STORE_REGION = "objectStore.region";
    private static final String OBJECT_STORE_BUCKET = "objectStore.bucket";
    private static final String OBJECT_STORE_PREFIX = "objectStore.prefix";
    private static final String OBJECT_STORE_PATH_STYLE = "objectStore.pathStyleAccess";
    private static final String OBJECT_STORE_TIMEOUT = "objectStore.requestTimeoutSeconds";
    private static final String OBJECT_STORE_MAX_CONNECTIONS = "objectStore.maxConnections";
    private static final String OBJECT_STORE_SECRET_RESOLVER = "objectStore.secretResolverClassName";
    private static final String OBJECT_STORE_ACCESS_KEY_REF = "objectStore.accessKeySecretRef";
    private static final String OBJECT_STORE_SECRET_KEY_REF = "objectStore.secretKeySecretRef";
    private static final String OBJECT_STORE_SESSION_TOKEN_REF = "objectStore.sessionTokenSecretRef";

    private static final String BK_DEPLOYMENT_ID = "bookkeeper.deploymentId";
    private static final String BK_ENABLED = "bookkeeper.enabled";
    private static final String BK_PROVIDER_SCOPE = "bookkeeper.providerScopeSha256";
    private static final String BK_LEDGER_ID_PREFIX_BITS = "bookkeeper.ledgerIdPrefixBits";
    private static final String BK_LEDGER_ID_PREFIX_VALUE = "bookkeeper.ledgerIdPrefixValue";
    private static final String BK_RESERVATION_ID = "bookkeeper.reservationId";
    private static final String BK_ENSEMBLE_SIZE = "bookkeeper.ensembleSize";
    private static final String BK_WRITE_QUORUM = "bookkeeper.writeQuorumSize";
    private static final String BK_ACK_QUORUM = "bookkeeper.ackQuorumSize";
    private static final String BK_DIGEST_TYPE = "bookkeeper.digestType";
    private static final String BK_PASSWORD_REF = "bookkeeper.passwordSecretRef";
    private static final String BK_PASSWORD_VERSION = "bookkeeper.passwordIdentityVersion";
    private static final String BK_MAX_ENTRIES_PER_LEDGER = "bookkeeper.maxEntriesPerLedger";
    private static final String BK_MAX_BYTES_PER_LEDGER = "bookkeeper.maxBytesPerLedger";
    private static final String BK_MAX_APPEND_RANGES_PER_LEDGER = "bookkeeper.maxAppendRangesPerLedger";
    private static final String BK_PROTECTION_SLOTS_PER_RANGE = "bookkeeper.protectionSlotsPerRange";
    private static final String BK_MAX_READER_LEASES_PER_LEDGER = "bookkeeper.maxReaderLeasesPerLedger";
    private static final String BK_MAX_UNCERTAIN_ALLOCATIONS = "bookkeeper.maxUncertainAllocations";
    private static final String BK_MAX_LEDGER_AGE_SECONDS = "bookkeeper.maxLedgerAgeSeconds";
    private static final String BK_MAX_WRITES_IN_FLIGHT = "bookkeeper.maxWritesInFlight";
    private static final String BK_MAX_READS_IN_FLIGHT = "bookkeeper.maxReadsInFlight";
    private static final String BK_MAX_READ_BYTES_IN_FLIGHT = "bookkeeper.maxReadBytesInFlight";
    private static final String BK_OPERATION_TIMEOUT_SECONDS = "bookkeeper.operationTimeoutSeconds";
    private static final String BK_ALLOCATION_TIMEOUT_SECONDS = "bookkeeper.allocationTimeoutSeconds";
    private static final String BK_SEAL_TIMEOUT_SECONDS = "bookkeeper.sealTimeoutSeconds";
    private static final String BK_DELETE_TIMEOUT_SECONDS = "bookkeeper.deleteTimeoutSeconds";
    private static final String BK_READER_LEASE_SECONDS = "bookkeeper.readerLeaseSeconds";
    private static final String BK_READER_LEASE_RENEW_SECONDS = "bookkeeper.readerLeaseRenewSeconds";
    private static final String BK_RETENTION_SCAN_INTERVAL_SECONDS = "bookkeeper.retentionScanIntervalSeconds";
    private static final String BK_RETENTION_SCAN_PAGE_SIZE = "bookkeeper.retentionScanPageSize";
    private static final String BK_GC_ENABLED = "bookkeeper.gc.enabled";
    private static final String BK_GC_DRY_RUN = "bookkeeper.gc.dryRun";
    private static final String BK_GC_MAX_CONCURRENT_DELETES = "bookkeeper.gc.maxConcurrentDeletes";
    private static final String BK_GC_MAX_CLOCK_SKEW_SECONDS = "bookkeeper.gc.maxClockSkewSeconds";
    private static final String BK_GC_DRAIN_GRACE_SECONDS = "bookkeeper.gc.drainGraceSeconds";
    private static final String BK_GC_LATE_CREATE_AUDIT_GRACE_SECONDS = "bookkeeper.gc.lateCreateAuditGraceSeconds";

    private static final String OPERATOR_EVIDENCE = "operatorEvidenceSha256";
    private static final Set<String> SUPPORTED_KEYS = Set.of(
            CLUSTER,
            OXIA_SERVICE_ADDRESS,
            OXIA_NAMESPACE,
            OXIA_SESSION_TIMEOUT_SECONDS,
            OXIA_MAX_PENDING_OPERATIONS,
            OBJECT_STORE_PROVIDER_CLASS,
            OBJECT_STORE_ENDPOINT,
            OBJECT_STORE_REGION,
            OBJECT_STORE_BUCKET,
            OBJECT_STORE_PREFIX,
            OBJECT_STORE_PATH_STYLE,
            OBJECT_STORE_TIMEOUT,
            OBJECT_STORE_MAX_CONNECTIONS,
            OBJECT_STORE_SECRET_RESOLVER,
            OBJECT_STORE_ACCESS_KEY_REF,
            OBJECT_STORE_SECRET_KEY_REF,
            OBJECT_STORE_SESSION_TOKEN_REF,
            BK_DEPLOYMENT_ID,
            BK_ENABLED,
            BK_PROVIDER_SCOPE,
            BK_LEDGER_ID_PREFIX_BITS,
            BK_LEDGER_ID_PREFIX_VALUE,
            BK_RESERVATION_ID,
            BK_ENSEMBLE_SIZE,
            BK_WRITE_QUORUM,
            BK_ACK_QUORUM,
            BK_DIGEST_TYPE,
            BK_PASSWORD_REF,
            BK_PASSWORD_VERSION,
            BK_MAX_ENTRIES_PER_LEDGER,
            BK_MAX_BYTES_PER_LEDGER,
            BK_MAX_APPEND_RANGES_PER_LEDGER,
            BK_PROTECTION_SLOTS_PER_RANGE,
            BK_MAX_READER_LEASES_PER_LEDGER,
            BK_MAX_UNCERTAIN_ALLOCATIONS,
            BK_MAX_LEDGER_AGE_SECONDS,
            BK_MAX_WRITES_IN_FLIGHT,
            BK_MAX_READS_IN_FLIGHT,
            BK_MAX_READ_BYTES_IN_FLIGHT,
            BK_OPERATION_TIMEOUT_SECONDS,
            BK_ALLOCATION_TIMEOUT_SECONDS,
            BK_SEAL_TIMEOUT_SECONDS,
            BK_DELETE_TIMEOUT_SECONDS,
            BK_READER_LEASE_SECONDS,
            BK_READER_LEASE_RENEW_SECONDS,
            BK_RETENTION_SCAN_INTERVAL_SECONDS,
            BK_RETENTION_SCAN_PAGE_SIZE,
            BK_GC_ENABLED,
            BK_GC_DRY_RUN,
            BK_GC_MAX_CONCURRENT_DELETES,
            BK_GC_MAX_CLOCK_SKEW_SECONDS,
            BK_GC_DRAIN_GRACE_SECONDS,
            BK_GC_LATE_CREATE_AUDIT_GRACE_SECONDS,
            OPERATOR_EVIDENCE);

    private final String cluster;
    private final OxiaClientConfiguration oxia;
    private final ObjectStoreConfiguration objectStore;
    private final String objectStoreSecretResolverClassName;
    private final Optional<NereusBookKeeperRuntimeConfiguration> bookKeeper;
    private final Optional<String> operatorEvidenceSha256;

    private AdminConfiguration(
            String cluster,
            OxiaClientConfiguration oxia,
            ObjectStoreConfiguration objectStore,
            String objectStoreSecretResolverClassName,
            Optional<NereusBookKeeperRuntimeConfiguration> bookKeeper,
            Optional<String> operatorEvidenceSha256) {
        this.cluster = Objects.requireNonNull(cluster);
        this.oxia = Objects.requireNonNull(oxia);
        this.objectStore = Objects.requireNonNull(objectStore);
        this.objectStoreSecretResolverClassName = Objects.requireNonNull(objectStoreSecretResolverClassName);
        this.bookKeeper = Objects.requireNonNull(bookKeeper);
        this.operatorEvidenceSha256 = Objects.requireNonNull(operatorEvidenceSha256);
    }

    public String cluster() {
        return cluster;
    }

    public OxiaClientConfiguration oxia() {
        return oxia;
    }

    public ObjectStoreConfiguration objectStore() {
        return objectStore;
    }

    public String objectStoreSecretResolverClassName() {
        return objectStoreSecretResolverClassName;
    }

    public NereusBookKeeperRuntimeConfiguration bookKeeper() {
        return bookKeeper.orElseThrow(() -> new IllegalStateException("BookKeeper administration is disabled"));
    }

    public Optional<NereusBookKeeperRuntimeConfiguration> optionalBookKeeper() {
        return bookKeeper;
    }

    public String operatorEvidenceSha256() {
        return operatorEvidenceSha256.orElseThrow(
                () -> new IllegalStateException("operatorEvidenceSha256 is required for BookKeeper administration"));
    }

    public static AdminConfiguration load(Path configFile) {
        Properties props = loadProperties(configFile);
        String unknownKeys = props.stringPropertyNames().stream()
                .filter(key -> !SUPPORTED_KEYS.contains(key))
                .sorted()
                .reduce((first, second) -> first + ", " + second)
                .orElse("");
        if (!unknownKeys.isEmpty()) {
            throw new IllegalStateException("unsupported config keys: " + unknownKeys);
        }

        String cluster = required(props, CLUSTER);

        String oxiaServiceAddress = required(props, OXIA_SERVICE_ADDRESS);
        String oxiaNamespace = required(props, OXIA_NAMESPACE);
        int oxiaSessionTimeout = intProp(props, OXIA_SESSION_TIMEOUT_SECONDS, 30);
        int oxiaMaxPending = intProp(props, OXIA_MAX_PENDING_OPERATIONS, 1024);
        OxiaClientConfiguration oxia = new OxiaClientConfiguration(
                oxiaServiceAddress,
                oxiaNamespace,
                Duration.ofSeconds(30),
                Duration.ofSeconds(oxiaSessionTimeout),
                10_000,
                oxiaMaxPending);

        String providerClass = required(props, OBJECT_STORE_PROVIDER_CLASS);
        URI endpoint = URI.create(required(props, OBJECT_STORE_ENDPOINT));
        String region = required(props, OBJECT_STORE_REGION);
        String bucket = required(props, OBJECT_STORE_BUCKET);
        String prefix = required(props, OBJECT_STORE_PREFIX);
        boolean pathStyle = boolProp(props, OBJECT_STORE_PATH_STYLE, true);
        int requestTimeout = intProp(props, OBJECT_STORE_TIMEOUT, 30);
        int maxConnections = intProp(props, OBJECT_STORE_MAX_CONNECTIONS, 64);
        Optional<String> accessKeyRef = optional(props, OBJECT_STORE_ACCESS_KEY_REF);
        Optional<String> secretKeyRef = optional(props, OBJECT_STORE_SECRET_KEY_REF);
        Optional<String> sessionTokenRef = optional(props, OBJECT_STORE_SESSION_TOKEN_REF);
        String secretResolverClassName = required(props, OBJECT_STORE_SECRET_RESOLVER);
        ObjectStoreConfiguration objectStore = new ObjectStoreConfiguration(
                providerClass,
                endpoint,
                region,
                bucket,
                prefix,
                pathStyle,
                Duration.ofSeconds(requestTimeout),
                maxConnections,
                accessKeyRef,
                secretKeyRef,
                sessionTokenRef);

        Optional<NereusBookKeeperRuntimeConfiguration> bookKeeper;
        Optional<String> operatorEvidence;
        if (boolProp(props, BK_ENABLED, true)) {
            String deploymentId = required(props, BK_DEPLOYMENT_ID);
            String providerScope = required(props, BK_PROVIDER_SCOPE);
            int prefixBits = intProp(props, BK_LEDGER_ID_PREFIX_BITS, 12);
            long prefixValue = longProp(props, BK_LEDGER_ID_PREFIX_VALUE, 2049L);
            String reservationId = required(props, BK_RESERVATION_ID);
            int ensembleSize = intProp(props, BK_ENSEMBLE_SIZE, 3);
            int writeQuorum = intProp(props, BK_WRITE_QUORUM, 3);
            int ackQuorum = intProp(props, BK_ACK_QUORUM, 2);
            String digestType = required(props, BK_DIGEST_TYPE);
            String passwordRef = required(props, BK_PASSWORD_REF);
            String passwordVersion = required(props, BK_PASSWORD_VERSION);

            BookKeeperWalConfiguration wal = new BookKeeperWalConfiguration(
                    cluster,
                    providerScope,
                    prefixBits,
                    prefixValue,
                    reservationId,
                    ensembleSize,
                    writeQuorum,
                    ackQuorum,
                    BookKeeperDigestType.valueOf(digestType),
                    new BookKeeperSecretRef(passwordRef, passwordVersion),
                    longProp(props, BK_MAX_ENTRIES_PER_LEDGER, 100_000L),
                    longProp(props, BK_MAX_BYTES_PER_LEDGER, 268_435_456L),
                    intProp(props, BK_MAX_APPEND_RANGES_PER_LEDGER, 1000),
                    intProp(props, BK_PROTECTION_SLOTS_PER_RANGE, 8),
                    intProp(props, BK_MAX_READER_LEASES_PER_LEDGER, 64),
                    intProp(props, BK_MAX_UNCERTAIN_ALLOCATIONS, 32),
                    seconds(props, BK_MAX_LEDGER_AGE_SECONDS, 3600),
                    intProp(props, BK_MAX_WRITES_IN_FLIGHT, 1),
                    intProp(props, BK_MAX_READS_IN_FLIGHT, 64),
                    longProp(props, BK_MAX_READ_BYTES_IN_FLIGHT, 134_217_728L),
                    seconds(props, BK_OPERATION_TIMEOUT_SECONDS, 30),
                    seconds(props, BK_ALLOCATION_TIMEOUT_SECONDS, 20),
                    seconds(props, BK_SEAL_TIMEOUT_SECONDS, 30),
                    seconds(props, BK_DELETE_TIMEOUT_SECONDS, 30),
                    seconds(props, BK_READER_LEASE_SECONDS, 120),
                    seconds(props, BK_READER_LEASE_RENEW_SECONDS, 30),
                    seconds(props, BK_RETENTION_SCAN_INTERVAL_SECONDS, 60),
                    intProp(props, BK_RETENTION_SCAN_PAGE_SIZE, 256));

            BookKeeperLedgerGcConfiguration gc = new BookKeeperLedgerGcConfiguration(
                    intProp(props, BK_GC_MAX_CONCURRENT_DELETES, 1),
                    seconds(props, BK_GC_MAX_CLOCK_SKEW_SECONDS, 5),
                    seconds(props, BK_GC_DRAIN_GRACE_SECONDS, 300),
                    seconds(props, BK_GC_LATE_CREATE_AUDIT_GRACE_SECONDS, 604_800),
                    boolProp(props, BK_GC_ENABLED, false),
                    boolProp(props, BK_GC_DRY_RUN, true));
            bookKeeper = Optional.of(new NereusBookKeeperRuntimeConfiguration(deploymentId, wal, gc));
            operatorEvidence = Optional.of(required(props, OPERATOR_EVIDENCE));
        } else {
            bookKeeper = Optional.empty();
            operatorEvidence = Optional.empty();
        }

        return new AdminConfiguration(
                cluster, oxia, objectStore, secretResolverClassName, bookKeeper, operatorEvidence);
    }

    private static Properties loadProperties(Path path) {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load config from " + path, e);
        }
        return props;
    }

    private static String required(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("missing required config key: " + key);
        }
        return value.strip();
    }

    private static Optional<String> optional(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(value.strip());
    }

    private static int intProp(Properties props, String key, int defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.strip());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("invalid integer for " + key + ": " + value);
        }
    }

    private static long longProp(Properties props, String key, long defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.strip());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("invalid long for " + key + ": " + value);
        }
    }

    private static boolean boolProp(Properties props, String key, boolean defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        String exact = value.strip();
        if ("true".equalsIgnoreCase(exact)) {
            return true;
        }
        if ("false".equalsIgnoreCase(exact)) {
            return false;
        }
        throw new IllegalStateException("invalid boolean for " + key + ": " + value);
    }

    private static Duration seconds(Properties props, String key, long defaultValue) {
        return Duration.ofSeconds(longProp(props, key, defaultValue));
    }
}
