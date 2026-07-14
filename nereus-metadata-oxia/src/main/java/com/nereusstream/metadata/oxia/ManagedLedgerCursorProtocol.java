/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ApiLimits;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Monotonic F3 minimum-reader marker carried by the authoritative F2 topic projection. */
public final class ManagedLedgerCursorProtocol {
    public static final String PROPERTY = "nereus.cursor-protocol";
    public static final String VERSION_1 = "1";

    private ManagedLedgerCursorProtocol() {
    }

    public static boolean isActivated(TopicProjectionRecord record) {
        Objects.requireNonNull(record, "record");
        return VERSION_1.equals(record.properties().get(PROPERTY));
    }

    public static Map<String, String> activate(Map<String, String> durableProperties) {
        Map<String, String> current = canonicalDurableProperties(durableProperties);
        if (VERSION_1.equals(current.get(PROPERTY))) {
            return current;
        }
        Map<String, String> activated = new LinkedHashMap<>(current);
        activated.put(PROPERTY, VERSION_1);
        return canonicalDurableProperties(activated);
    }

    public static Map<String, String> canonicalDurableProperties(Map<String, String> durableProperties) {
        Map<String, String> canonical = MetadataCanonicalizer.canonicalStringMap(
                durableProperties == null ? Map.of() : durableProperties,
                ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES,
                "managedLedgerDurableProperties");
        for (Map.Entry<String, String> entry : canonical.entrySet()) {
            String key = entry.getKey();
            if (key.equals(PROPERTY)) {
                if (!VERSION_1.equals(entry.getValue())) {
                    throw new IllegalArgumentException("unsupported Nereus cursor protocol marker");
                }
            } else if (key.startsWith("nereus.") || key.equals("PULSAR.SHADOW_SOURCE")) {
                throw new IllegalArgumentException("managed-ledger property key is reserved: " + key);
            }
        }
        return canonical;
    }

    public static Map<String, String> externalProperties(Map<String, String> durableProperties) {
        Map<String, String> durable = canonicalDurableProperties(durableProperties);
        if (!durable.containsKey(PROPERTY)) {
            return durable;
        }
        Map<String, String> external = new LinkedHashMap<>(durable);
        external.remove(PROPERTY);
        return Map.copyOf(external);
    }

    public static Map<String, String> replaceExternalProperties(
            Map<String, String> currentDurableProperties,
            Map<String, String> requestedExternalProperties) {
        Map<String, String> current = canonicalDurableProperties(currentDurableProperties);
        Map<String, String> replacement = new LinkedHashMap<>(
                ProjectionCreateRequest.canonicalProperties(requestedExternalProperties));
        if (VERSION_1.equals(current.get(PROPERTY))) {
            replacement.put(PROPERTY, VERSION_1);
        }
        return canonicalDurableProperties(replacement);
    }
}
