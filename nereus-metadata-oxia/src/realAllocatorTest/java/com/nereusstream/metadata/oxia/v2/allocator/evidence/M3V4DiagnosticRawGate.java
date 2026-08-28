/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Independent fail-closed checks over the V4 RANGE raw receipts required by the diagnostic gate. */
final class M3V4DiagnosticRawGate {
    private static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_RANGE_LATENCY_DIAGNOSTIC_V4";
    private static final Pattern SOURCE = Pattern.compile("\\\"sourceCommit\\\":\\\"([0-9a-f]{40})\\\"");
    private static final Pattern LATENCY = Pattern.compile("\\\"latencyMillis\\\":([0-9]+)");

    private M3V4DiagnosticRawGate() {}

    static Sha256Digest validate(Path directory, String sourceCommit) throws Exception {
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(directory)) {
            throw new IllegalArgumentException("allocator V4 diagnostic raw directory is absent or a link");
        }
        StringBuilder manifest = new StringBuilder("NEREUS_V2_M3_ALLOCATOR_DIAGNOSTIC_RAW_MANIFEST_V4\n");
        for (int latencyMillis : new int[] {10, 25}) {
            Path receipt = directory.resolve("v4-range1024-" + latencyMillis + "ms-formal-sequence.json");
            byte[] bytes = M3V3AllocatorProtocolMain.readRegular(receipt, 16 * 1024 * 1024);
            String json = new String(bytes, StandardCharsets.UTF_8);
            requireLiteral(json, "\"schema\":\"" + SCHEMA + "\"");
            requireLiteral(json, "\"diagnosticOnly\":true");
            requireLiteral(json, "\"authority\":false");
            requireLiteral(json, "\"selectionEligible\":false");
            if (!match(SOURCE, json, "source commit").group(1).equals(sourceCommit)) {
                throw new IllegalArgumentException("allocator V4 diagnostic raw source commit differs");
            }
            if (Integer.parseInt(match(LATENCY, json, "latency").group(1)) != latencyMillis) {
                throw new IllegalArgumentException("allocator V4 diagnostic raw latency differs");
            }
            requireLosslessRow(json, "fixed1000", 1_000);
            requireLosslessRow(json, "derived800", 800);
            manifest.append(receipt.getFileName())
                    .append('\0')
                    .append(bytes.length)
                    .append('\0')
                    .append(Sha256Digest.hash(CanonicalBytes.copyOf(bytes)).toHex())
                    .append('\n');
        }
        return Sha256Digest.hash(CanonicalBytes.copyOf(manifest.toString().getBytes(StandardCharsets.UTF_8)));
    }

    private static void requireLosslessRow(String json, String name, int offeredRate) {
        Pattern row = Pattern.compile("\\\"" + name + "\\\":\\{"
                + "\\\"offeredRate\\\":" + offeredRate
                + ",\\\"offered\\\":([0-9]+)"
                + ",\\\"admitted\\\":([0-9]+)"
                + ",\\\"dropped\\\":([0-9]+)"
                + ",\\\"completed\\\":([0-9]+)"
                + ",\\\"failed\\\":([0-9]+)"
                + ",\\\"timedOut\\\":([0-9]+)");
        Matcher matcher = match(row, json, name);
        long offered = Long.parseLong(matcher.group(1));
        long admitted = Long.parseLong(matcher.group(2));
        long dropped = Long.parseLong(matcher.group(3));
        long completed = Long.parseLong(matcher.group(4));
        long failed = Long.parseLong(matcher.group(5));
        long timedOut = Long.parseLong(matcher.group(6));
        if (offered != Math.multiplyExact((long) offeredRate, 30L)
                || admitted != offered
                || completed != offered
                || dropped != 0
                || failed != 0
                || timedOut != 0) {
            throw new IllegalArgumentException("allocator V4 diagnostic raw " + name + " hard gate failed");
        }
    }

    private static void requireLiteral(String json, String literal) {
        if (!json.contains(literal)) {
            throw new IllegalArgumentException("allocator V4 diagnostic raw identity differs: " + literal);
        }
    }

    private static Matcher match(Pattern pattern, String json, String label) {
        Matcher matcher = pattern.matcher(json);
        if (!matcher.find()) {
            throw new IllegalArgumentException("allocator V4 diagnostic raw " + label + " is absent");
        }
        return matcher;
    }
}
