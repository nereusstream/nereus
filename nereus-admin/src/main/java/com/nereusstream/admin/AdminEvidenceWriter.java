/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;

public final class AdminEvidenceWriter {

    private AdminEvidenceWriter() {}

    public static String namespaceReservationEvidence(
            String command, BookKeeperLedgerIdNamespaceReservation reservation) {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("schemaVersion", 1);
        evidence.put("command", command);
        evidence.put("status", reservation.lifecycle().name());
        evidence.put("cluster", reservation.clusterAlias());
        evidence.put("deploymentId", reservation.nereusDeploymentId());
        evidence.put("providerScopeSha256", reservation.bookKeeperProviderScopeSha256());
        evidence.put("ledgerIdPrefixBits", reservation.ledgerIdPrefixBits());
        evidence.put("ledgerIdPrefixValue", reservation.ledgerIdPrefixValue());
        evidence.put("reservationId", reservation.reservationId());
        evidence.put(
                "ledgerIdNamespaceSha256", reservation.ledgerIdNamespaceSha256().value());
        evidence.put("metadataVersion", reservation.metadataVersion());
        evidence.put("completedAtEpochMillis", Clock.systemUTC().millis());
        return toJson(evidence);
    }

    public static String activationReadEvidence(String command, BookKeeperProtocolActivation activation) {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("schemaVersion", 1);
        evidence.put("command", command);
        evidence.put("presence", true);
        evidence.put("lifecycle", activation.value().lifecycle().name());
        evidence.put("metadataVersion", activation.metadataVersion());
        evidence.put("readinessEpoch", activation.value().brokerReadinessEpoch());
        evidence.put("readinessSha256", activation.value().brokerReadinessSha256());
        evidence.put("walOnlyPublicationEnabled", activation.value().walOnlyPublicationEnabled());
        evidence.put("asyncPublicationEnabled", activation.value().asyncPublicationEnabled());
        evidence.put("syncPublicationEnabled", activation.value().syncPublicationEnabled());
        evidence.put("ledgerDeletionEnabled", activation.value().ledgerDeletionEnabled());
        evidence.put(
                "publicationActivationSha256",
                activation.supportsAllPublications()
                        ? activation.publicationActivationSha256().value()
                        : null);
        evidence.put("completedAtEpochMillis", Clock.systemUTC().millis());
        return toJson(evidence);
    }

    public static void writeEvidence(Path outputFile, String evidence) {
        try {
            Files.writeString(outputFile, evidence + "\n", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("failed to write evidence to " + outputFile, e);
        }
    }

    public static void printEvidence(String evidence) {
        System.out.println(evidence);
        System.out.flush();
    }

    private static String toJson(Map<String, Object> evidence) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        boolean first = true;
        for (Map.Entry<String, Object> entry : evidence.entrySet()) {
            if (!first) {
                sb.append(",\n");
            }
            first = false;
            sb.append("  \"");
            sb.append(entry.getKey());
            sb.append("\": ");
            appendValue(sb, entry.getValue());
        }
        sb.append("\n}");
        return sb.toString();
    }

    private static void appendValue(StringBuilder sb, Object value) {
        if (value instanceof String s) {
            sb.append('"');
            sb.append(escape(s));
            sb.append('"');
        } else if (value instanceof Boolean b) {
            sb.append(b);
        } else if (value instanceof Number n) {
            sb.append(n);
        } else if (value == null) {
            sb.append("null");
        } else {
            sb.append('"');
            sb.append(escape(String.valueOf(value)));
            sb.append('"');
        }
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
