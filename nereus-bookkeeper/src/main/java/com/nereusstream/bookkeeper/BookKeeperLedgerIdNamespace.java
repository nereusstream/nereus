/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.security.SecureRandom;
import java.util.Objects;
import java.util.random.RandomGenerator;

/** Exact positive-63-bit advanced-ledger-id namespace reserved outside the runtime. */
public final class BookKeeperLedgerIdNamespace {
    private final int prefixBits;
    private final long prefixValue;
    private final int suffixBits;
    private final long suffixMask;

    public BookKeeperLedgerIdNamespace(int prefixBits, long prefixValue) {
        this.prefixBits = prefixBits;
        this.prefixValue = prefixValue;
        if (prefixBits < 8 || prefixBits > 24) throw new IllegalArgumentException("prefixBits must be in [8,24]");
        long limit = 1L << prefixBits;
        if (prefixValue < (limit >>> 1) || prefixValue >= limit) {
            throw new IllegalArgumentException("prefixValue must fit and set its highest bit");
        }
        suffixBits = 63 - prefixBits;
        suffixMask = (1L << suffixBits) - 1;
    }

    public int prefixBits() { return prefixBits; }
    public long prefixValue() { return prefixValue; }
    public int suffixBits() { return suffixBits; }

    public boolean contains(long ledgerId) {
        return ledgerId > 0 && (ledgerId >>> suffixBits) == prefixValue;
    }

    public long candidate() { return candidate(new SecureRandom()); }

    public long candidate(RandomGenerator random) {
        Objects.requireNonNull(random, "random");
        long suffix = random.nextLong() & suffixMask;
        long candidate = (prefixValue << suffixBits) | suffix;
        if (!contains(candidate)) throw new IllegalStateException("generated ledger id failed namespace round-trip");
        return candidate;
    }
}
