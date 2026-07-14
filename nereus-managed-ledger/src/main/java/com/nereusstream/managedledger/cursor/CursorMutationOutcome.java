/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

/** Whether one durable cursor request changed the authoritative root. */
public enum CursorMutationOutcome {
    APPLIED,
    ALREADY_APPLIED
}
