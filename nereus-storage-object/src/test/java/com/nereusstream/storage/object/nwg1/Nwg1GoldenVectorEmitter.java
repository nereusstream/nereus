/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

/** Explicit temp-only, double-emission NWG1 exact-corpus emitter. */
public final class Nwg1GoldenVectorEmitter {
    private Nwg1GoldenVectorEmitter() {}

    public static void main(String[] arguments) throws IOException {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("usage: emitter <explicit-empty-temp-directory>");
        }
        Path output = Path.of(arguments[0]).toAbsolutePath().normalize();
        Path repository =
                Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        if (!output.isAbsolute() || output.startsWith(repository)) {
            throw new IllegalArgumentException("emitter output must be an explicit directory outside the repository");
        }
        Files.createDirectories(output);
        try (var children = Files.list(output)) {
            if (children.findAny().isPresent()) {
                throw new IllegalArgumentException("emitter output must be empty");
            }
        }
        Path first = output.resolve("first");
        Path second = output.resolve("second");
        emit(first);
        emit(second);
        compareRecursively(first, second);
        Files.writeString(output.resolve("DOUBLE_EMISSION_IDENTICAL"), "true\n", StandardCharsets.US_ASCII);
    }

    private static void emit(Path target) throws IOException {
        Files.createDirectories(target);
        Files.writeString(
                target.resolve("nwg1-v1-positive-inputs.json"),
                StrictJcsV1.encode(Nwg1GoldenCorpusV1.positiveInputProjection()),
                StandardCharsets.UTF_8);
        StringBuilder tsv = new StringBuilder("vectorId\tcomponentKind\tordinal\tlength\tsha256\thex\n");
        for (Nwg1GoldenCorpusV1.Vector vector : Nwg1GoldenCorpusV1.vectors()) {
            for (Nwg1GoldenCorpusV1.Component component : vector.components()) {
                byte[] bytes = component.bytes();
                String exactHex = HexFormat.of().formatHex(bytes);
                tsv.append(component.vectorId())
                        .append('\t')
                        .append(component.kind())
                        .append('\t')
                        .append(component.ordinal())
                        .append('\t')
                        .append(bytes.length)
                        .append('\t')
                        .append(HexFormat.of().formatHex(Nwg1CommitmentsV1.sha256(bytes)))
                        .append('\t')
                        .append(exactHex.isEmpty() ? "\"\"" : exactHex)
                        .append('\n');
            }
        }
        Files.writeString(target.resolve("nwg1-v1-goldens.tsv"), tsv.toString(), StandardCharsets.US_ASCII);
    }

    private static void compareRecursively(Path first, Path second) throws IOException {
        List<Path> firstFiles = relativeFiles(first);
        List<Path> secondFiles = relativeFiles(second);
        if (!firstFiles.equals(secondFiles)) {
            throw new IllegalStateException("double emission file inventory differs");
        }
        for (Path relative : firstFiles) {
            if (!MessageDigest.isEqual(
                    Files.readAllBytes(first.resolve(relative)), Files.readAllBytes(second.resolve(relative)))) {
                throw new IllegalStateException("double emission differs: " + relative);
            }
        }
    }

    private static List<Path> relativeFiles(Path root) throws IOException {
        List<Path> result = new ArrayList<>();
        try (var paths = Files.walk(root)) {
            paths.filter(Files::isRegularFile)
                    .map(root::relativize)
                    .sorted(Comparator.comparing(Path::toString))
                    .forEach(result::add);
        }
        return result;
    }
}
