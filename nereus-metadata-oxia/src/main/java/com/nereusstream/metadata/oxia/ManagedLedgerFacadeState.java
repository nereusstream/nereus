/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

/** Monotonic facade lifecycle mirror; L0 stream state remains authoritative. */
public enum ManagedLedgerFacadeState {
    OPEN,
    SEALED,
    DELETING,
    DELETED
}
