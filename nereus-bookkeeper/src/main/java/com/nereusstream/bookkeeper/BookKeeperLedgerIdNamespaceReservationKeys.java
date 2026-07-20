/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.util.Locale;
import java.util.Objects;

/** Strict canonical Oxia key and partition identity for one shared-provider ledger-id prefix. */
public final class BookKeeperLedgerIdNamespaceReservationKeys {
    private static final String ROOT = "/nereus/bookkeeper-provider-scopes/v1/";

    private BookKeeperLedgerIdNamespaceReservationKeys() {
    }

    public static String key(String providerScopeSha256, int prefixBits, long prefixValue) {
        String scope = BookKeeperWalConfiguration.sha256(
                providerScopeSha256, "providerScopeSha256");
        if (!scope.equals(providerScopeSha256)) {
            throw new IllegalArgumentException("providerScopeSha256 must be canonical lowercase hex");
        }
        new BookKeeperLedgerIdNamespace(prefixBits, prefixValue);
        String key = ROOT
                + scope
                + "/ledger-id-prefixes/"
                + String.format(Locale.ROOT, "%02d/%06x", prefixBits, prefixValue);
        requireExact(key, scope, prefixBits, prefixValue);
        return key;
    }

    public static String partitionKey(String providerScopeSha256) {
        String scope = BookKeeperWalConfiguration.sha256(
                providerScopeSha256, "providerScopeSha256");
        if (!scope.equals(providerScopeSha256)) {
            throw new IllegalArgumentException("providerScopeSha256 must be canonical lowercase hex");
        }
        return "bookkeeper-provider-scope-v1-" + scope;
    }

    public static void requireExact(
            String rawKey,
            String providerScopeSha256,
            int prefixBits,
            long prefixValue) {
        String supplied = Objects.requireNonNull(rawKey, "rawKey");
        String scope = BookKeeperWalConfiguration.sha256(
                providerScopeSha256, "providerScopeSha256");
        if (!scope.equals(providerScopeSha256)) {
            throw new IllegalArgumentException("providerScopeSha256 must be canonical lowercase hex");
        }
        new BookKeeperLedgerIdNamespace(prefixBits, prefixValue);
        String expected = ROOT
                + scope
                + "/ledger-id-prefixes/"
                + String.format(Locale.ROOT, "%02d/%06x", prefixBits, prefixValue);
        if (!expected.equals(supplied)) {
            throw new IllegalArgumentException("BookKeeper namespace reservation key is not canonical");
        }
    }
}
