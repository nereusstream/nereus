/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Monotonic F4 minimum-reader marker carried by the authoritative F2 topic projection. */
public final class ManagedLedgerGenerationProtocol {
    public static final String PROPERTY = "nereus.generation-protocol";
    public static final String VERSION_1 = "1";

    private ManagedLedgerGenerationProtocol() {
    }

    public static boolean isActivated(TopicProjectionRecord record) {
        Objects.requireNonNull(record, "record");
        return VERSION_1.equals(record.properties().get(PROPERTY));
    }

    public static Map<String, String> activate(Map<String, String> durableProperties) {
        Map<String, String> current =
                ManagedLedgerProtocolProperties.canonicalDurableProperties(durableProperties);
        if (VERSION_1.equals(current.get(PROPERTY))) {
            return current;
        }
        Map<String, String> activated = new LinkedHashMap<>(current);
        activated.put(PROPERTY, VERSION_1);
        return ManagedLedgerProtocolProperties.canonicalDurableProperties(activated);
    }
}
