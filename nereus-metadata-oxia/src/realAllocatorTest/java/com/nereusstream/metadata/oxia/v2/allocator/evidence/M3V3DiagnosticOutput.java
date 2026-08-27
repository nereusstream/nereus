/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** Fail-closed writer for diagnostic-only V3 output; no formal evidence schema is emitted here. */
final class M3V3DiagnosticOutput {
    private M3V3DiagnosticOutput() {}

    static void writeNew(String fileName, String json) throws IOException {
        Objects.requireNonNull(fileName, "fileName");
        Objects.requireNonNull(json, "json");
        if (!fileName.matches("[a-z0-9-]+\\.json")) {
            throw new IllegalArgumentException("allocator V3 diagnostic output filename is unsafe");
        }
        Path directory = Path.of(requiredProperty("nereus.m3.allocator.v3.diagnosticOutput"))
                .toAbsolutePath()
                .normalize();
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(directory)) {
            throw new IllegalStateException("allocator V3 diagnostic output directory is absent or nonregular");
        }
        Path output = directory.resolve(fileName).normalize();
        if (!output.getParent().equals(directory)) {
            throw new IllegalArgumentException("allocator V3 diagnostic output escaped its directory");
        }
        Files.writeString(
                output,
                json,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
    }

    static String requiredProperty(String name) {
        String value = System.getProperty(name, "UNSET");
        if (value.isBlank() || value.equals("UNSET")) {
            throw new IllegalArgumentException("allocator V3 diagnostic property is absent: " + name);
        }
        return value;
    }

    static String jsonString(String value) {
        Objects.requireNonNull(value, "value");
        return '"' + value.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }
}
