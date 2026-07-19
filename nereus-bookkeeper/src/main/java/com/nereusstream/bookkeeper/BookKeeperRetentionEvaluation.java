/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** One complete gate result; blockers are explicit and an admitted result carries the exact frozen candidate. */
public record BookKeeperRetentionEvaluation(
        Set<BookKeeperRetentionBlocker> blockers,
        Optional<BookKeeperLedgerRetirementCandidate> candidate) {
    public BookKeeperRetentionEvaluation {
        Objects.requireNonNull(blockers, "blockers");
        blockers = blockers.isEmpty()
                ? Set.of()
                : Set.copyOf(EnumSet.copyOf(blockers));
        candidate = Objects.requireNonNull(candidate, "candidate");
        if (blockers.isEmpty() != candidate.isPresent()) {
            throw new IllegalArgumentException("retention evaluation must be either admitted or blocked");
        }
    }

    public static BookKeeperRetentionEvaluation blocked(Set<BookKeeperRetentionBlocker> blockers) {
        if (Objects.requireNonNull(blockers, "blockers").isEmpty()) {
            throw new IllegalArgumentException("blocked evaluation requires at least one reason");
        }
        return new BookKeeperRetentionEvaluation(blockers, Optional.empty());
    }

    public static BookKeeperRetentionEvaluation admitted(BookKeeperLedgerRetirementCandidate candidate) {
        return new BookKeeperRetentionEvaluation(Set.of(), Optional.of(candidate));
    }
}
