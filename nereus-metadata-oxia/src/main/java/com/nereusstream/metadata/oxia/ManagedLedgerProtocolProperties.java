/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ApiLimits;
import com.nereusstream.api.MetadataCanonicalizer;
import java.util.LinkedHashMap;
import java.util.Map;

/** Canonical composed validator for internal managed-ledger protocol markers. */
public final class ManagedLedgerProtocolProperties {
    private ManagedLedgerProtocolProperties() {
    }

    public static Map<String, String> canonicalDurableProperties(
            Map<String, String> durableProperties) {
        Map<String, String> canonical = MetadataCanonicalizer.canonicalStringMap(
                durableProperties == null ? Map.of() : durableProperties,
                ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES,
                "managedLedgerDurableProperties");
        for (Map.Entry<String, String> entry : canonical.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equals(ManagedLedgerCursorProtocol.PROPERTY)) {
                requireVersion(value, ManagedLedgerCursorProtocol.VERSION_1, "cursor");
            } else if (key.equals(ManagedLedgerGenerationProtocol.PROPERTY)) {
                requireVersion(value, ManagedLedgerGenerationProtocol.VERSION_1, "generation");
            } else if (key.startsWith("nereus.") || key.equals("PULSAR.SHADOW_SOURCE")) {
                throw new IllegalArgumentException(
                        "managed-ledger property key is reserved: " + key);
            }
        }
        return canonical;
    }

    public static Map<String, String> externalProperties(
            Map<String, String> durableProperties) {
        Map<String, String> durable = canonicalDurableProperties(durableProperties);
        if (!durable.containsKey(ManagedLedgerCursorProtocol.PROPERTY)
                && !durable.containsKey(ManagedLedgerGenerationProtocol.PROPERTY)) {
            return durable;
        }
        Map<String, String> external = new LinkedHashMap<>(durable);
        external.remove(ManagedLedgerCursorProtocol.PROPERTY);
        external.remove(ManagedLedgerGenerationProtocol.PROPERTY);
        return Map.copyOf(external);
    }

    public static Map<String, String> replaceExternalProperties(
            Map<String, String> currentDurableProperties,
            Map<String, String> requestedExternalProperties) {
        Map<String, String> current = canonicalDurableProperties(currentDurableProperties);
        Map<String, String> replacement = new LinkedHashMap<>(
                ProjectionCreateRequest.canonicalProperties(requestedExternalProperties));
        preserveMarker(current, replacement, ManagedLedgerCursorProtocol.PROPERTY);
        preserveMarker(current, replacement, ManagedLedgerGenerationProtocol.PROPERTY);
        return canonicalDurableProperties(replacement);
    }

    private static void preserveMarker(
            Map<String, String> current,
            Map<String, String> replacement,
            String property) {
        String value = current.get(property);
        if (value != null) {
            replacement.put(property, value);
        }
    }

    private static void requireVersion(String actual, String expected, String protocol) {
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException(
                    "unsupported Nereus " + protocol + " protocol marker");
        }
    }
}
