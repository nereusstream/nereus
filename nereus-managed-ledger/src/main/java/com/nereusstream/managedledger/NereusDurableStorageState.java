/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

public enum NereusDurableStorageState {
    MISSING,
    ACTIVE,
    SEALED,
    DELETING,
    DELETED
}
