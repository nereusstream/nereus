/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.Objects;

/** Opaque, scope- and page-size-bound continuation for BK metadata scans. */
public final class BookKeeperScanToken {
    private final String cluster;
    private final BookKeeperScanKind kind;
    private final String scopeIdentitySha256;
    private final String scanPrefix;
    private final String exclusiveLastKey;
    private final int pageSize;

    BookKeeperScanToken(
            String cluster,
            BookKeeperScanKind kind,
            String scopeIdentitySha256,
            String scanPrefix,
            String exclusiveLastKey,
            int pageSize) {
        this.cluster = text(cluster, "cluster");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.scopeIdentitySha256 = sha256(scopeIdentitySha256);
        this.scanPrefix = text(scanPrefix, "scanPrefix");
        this.exclusiveLastKey = text(exclusiveLastKey, "exclusiveLastKey");
        if (!exclusiveLastKey.startsWith(scanPrefix)) {
            throw new IllegalArgumentException("exclusiveLastKey is outside scanPrefix");
        }
        if (pageSize <= 0 || pageSize > 1_024) {
            throw new IllegalArgumentException("pageSize must be in [1,1024]");
        }
        this.pageSize = pageSize;
    }

    String cluster() {
        return cluster;
    }

    BookKeeperScanKind kind() {
        return kind;
    }

    String scopeIdentitySha256() {
        return scopeIdentitySha256;
    }

    String scanPrefix() {
        return scanPrefix;
    }

    int pageSize() {
        return pageSize;
    }

    String resumeFromInclusive() {
        return exclusiveLastKey + '\0';
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }

    private static String sha256(String value) {
        text(value, "scopeIdentitySha256");
        if (!value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("scopeIdentitySha256 must be lowercase SHA-256 hex");
        }
        return value;
    }
}
