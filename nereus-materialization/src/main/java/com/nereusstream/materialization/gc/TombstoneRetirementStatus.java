/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

/** Outcome of one bounded DELETED-root audit-retirement pass. */
public enum TombstoneRetirementStatus {
    DISABLED,
    DRY_RUN,
    RETIRED,
    NOT_OLD_ENOUGH,
    OWNER_PRESENT,
    HANDLE_PRESENT,
    DOMAIN_VETO,
    OBJECT_PRESENT,
    VERSION_CHANGED,
    QUARANTINED
}
