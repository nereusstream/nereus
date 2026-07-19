/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

/** One bounded convergence action; the caller schedules later passes instead of blocking a worker thread. */
public enum BookKeeperLedgerGcAction {
    DISABLED,
    DRY_RUN_ADMITTED,
    BLOCKED,
    MARKED,
    WAITING_DRAIN,
    UNMARKED,
    DELETING,
    DELETE_RETRY_REQUIRED,
    FIRST_ABSENCE_RECORDED,
    WAITING_SECOND_ABSENCE,
    DELETED,
    QUARANTINED,
    ALREADY_TERMINAL
}
