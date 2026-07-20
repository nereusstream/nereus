/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Canonical activation identity; a new physical binding receives a new immutable authority key. */
public final class BookKeeperProtocolActivationKeys {
    private static final String ROOT = "/nereus/bookkeeper-primary-wal/v1/clusters/";

    private BookKeeperProtocolActivationKeys() {
    }

    public static String key(
            String clusterAlias,
            String configurationBindingSha256,
            String ledgerIdNamespaceSha256) {
        String cluster = clusterSha256(clusterAlias);
        String configuration = sha(configurationBindingSha256, "configurationBindingSha256");
        String namespace = sha(ledgerIdNamespaceSha256, "ledgerIdNamespaceSha256");
        String key = ROOT
                + cluster
                + "/bindings/"
                + configuration
                + "/"
                + namespace
                + "/activation";
        requireExact(key, clusterAlias, configuration, namespace);
        return key;
    }

    public static String partitionKey(String clusterAlias) {
        return "bookkeeper-primary-wal-v1-" + clusterSha256(clusterAlias);
    }

    public static void requireExact(
            String supplied,
            String clusterAlias,
            String configurationBindingSha256,
            String ledgerIdNamespaceSha256) {
        String cluster = clusterSha256(clusterAlias);
        String configuration = sha(configurationBindingSha256, "configurationBindingSha256");
        String namespace = sha(ledgerIdNamespaceSha256, "ledgerIdNamespaceSha256");
        String expected = ROOT
                + cluster
                + "/bindings/"
                + configuration
                + "/"
                + namespace
                + "/activation";
        if (!expected.equals(Objects.requireNonNull(supplied, "supplied"))) {
            throw new IllegalArgumentException("BookKeeper activation key is not canonical");
        }
    }

    private static String clusterSha256(String clusterAlias) {
        String cluster = Objects.requireNonNull(clusterAlias, "clusterAlias");
        if (cluster.isBlank()) {
            throw new IllegalArgumentException("clusterAlias cannot be blank");
        }
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(cluster.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static String sha(String value, String name) {
        String exact = BookKeeperWalConfiguration.sha256(value, name);
        if (!exact.equals(value)) {
            throw new IllegalArgumentException(name + " must be canonical lowercase hex");
        }
        return exact;
    }
}
