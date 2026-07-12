/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record ManagedLedgerOpenConfigView(
        ManagedLedgerConfigView operationView,
        boolean createIfMissing,
        Map<String, String> initialProperties) {
    public ManagedLedgerOpenConfigView {
        Objects.requireNonNull(operationView, "operationView");
        initialProperties = Collections.unmodifiableMap(
                new LinkedHashMap<>(Objects.requireNonNull(initialProperties, "initialProperties")));
    }
}
