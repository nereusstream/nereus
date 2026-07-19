/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Observable result from one nonblocking GC convergence pass. */
public record BookKeeperLedgerGcResult(
        BookKeeperLedgerGcAction action,
        Optional<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> root,
        Set<BookKeeperRetentionBlocker> blockers) {
    public BookKeeperLedgerGcResult {
        Objects.requireNonNull(action, "action");
        root = Objects.requireNonNull(root, "root");
        blockers = Objects.requireNonNull(blockers, "blockers").isEmpty()
                ? Set.of()
                : Set.copyOf(EnumSet.copyOf(blockers));
        if ((action == BookKeeperLedgerGcAction.BLOCKED) != !blockers.isEmpty()) {
            throw new IllegalArgumentException("only BLOCKED GC results carry blockers");
        }
    }

    public static BookKeeperLedgerGcResult of(
            BookKeeperLedgerGcAction action,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root) {
        return new BookKeeperLedgerGcResult(action, Optional.ofNullable(root), Set.of());
    }

    public static BookKeeperLedgerGcResult blocked(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Set<BookKeeperRetentionBlocker> blockers) {
        return new BookKeeperLedgerGcResult(
                BookKeeperLedgerGcAction.BLOCKED, Optional.ofNullable(root), blockers);
    }
}
