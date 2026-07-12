/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.managedledger.NereusManagedLedgerRuntime;

/** Product-owned construction boundary used by the Pulsar fork. */
public interface NereusRuntimeProvider {
    NereusManagedLedgerRuntime create(
            NereusRuntimeConfiguration configuration,
            NereusRuntimeContext context) throws Exception;
}
