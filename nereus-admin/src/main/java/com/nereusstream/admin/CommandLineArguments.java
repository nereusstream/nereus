/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class CommandLineArguments {

    private static final Set<List<String>> SUPPORTED_COMMANDS = Set.of(
            List.of("bookkeeper", "namespace", "ensure"),
            List.of("bookkeeper", "namespace", "verify"),
            List.of("bookkeeper", "activation", "read"),
            List.of("object-store", "verify"),
            List.of("object-store", "contract"),
            List.of("object-store", "persistence", "create"),
            List.of("object-store", "persistence", "verify"),
            List.of("object-store", "persistence", "cleanup"));
    private static final Set<String> SUPPORTED_OPTIONS = Set.of(
            "config", "timeout-seconds", "output", "run-id");

    private final List<String> commandPath;
    private final Path config;
    private final long timeoutSeconds;
    private final Optional<Path> outputFile;
    private final Optional<String> runId;

    private CommandLineArguments(
            List<String> commandPath,
            Path config,
            long timeoutSeconds,
            Optional<Path> outputFile,
            Optional<String> runId) {
        this.commandPath = List.copyOf(Objects.requireNonNull(commandPath));
        this.config = Objects.requireNonNull(config);
        this.timeoutSeconds = timeoutSeconds;
        this.outputFile = Objects.requireNonNull(outputFile);
        this.runId = Objects.requireNonNull(runId);
    }

    public String command() {
        return commandPath.get(0);
    }

    public String subcommand() {
        return commandPath.get(1);
    }

    public Optional<String> action() {
        return commandPath.size() == 3
                ? Optional.of(commandPath.get(2))
                : Optional.empty();
    }

    public List<String> commandPath() {
        return commandPath;
    }

    public Path config() {
        return config;
    }

    public Duration timeout() {
        return Duration.ofSeconds(timeoutSeconds);
    }

    public long timeoutSeconds() {
        return timeoutSeconds;
    }

    public Optional<Path> outputFile() {
        return outputFile;
    }

    public Optional<String> runId() {
        return runId;
    }

    public static CommandLineArguments parse(String[] args) {
        Objects.requireNonNull(args, "args");
        int optionsOffset = firstOptionOffset(args);
        if (optionsOffset < 2) {
            throw new IllegalArgumentException(
                    "usage: nereus-admin <command> <subcommand> [action] "
                            + "--config <path> [--timeout-seconds <n>] [--output <path>]");
        }
        List<String> commandPath = Collections.unmodifiableList(
                Arrays.asList(Arrays.copyOfRange(args, 0, optionsOffset)));
        if (!SUPPORTED_COMMANDS.contains(commandPath)) {
            throw new IllegalArgumentException(
                    "unknown command: " + String.join(" ", commandPath));
        }

        Map<String, String> options = parseOptions(args, optionsOffset);
        if (!options.containsKey("config")) {
            throw new IllegalArgumentException("--config is required");
        }
        Path config = Path.of(options.get("config"));
        if (!Files.isReadable(config)) {
            throw new IllegalArgumentException("config file is not readable: " + config);
        }

        long timeoutSeconds = 600;
        if (options.containsKey("timeout-seconds")) {
            try {
                timeoutSeconds = Long.parseLong(options.get("timeout-seconds"));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "invalid timeout-seconds: " + options.get("timeout-seconds"));
            }
        }
        if (timeoutSeconds <= 0) {
            throw new IllegalArgumentException("timeout-seconds must be positive");
        }

        Optional<Path> output = Optional.ofNullable(options.get("output")).map(Path::of);
        Optional<String> runId = Optional.ofNullable(options.get("run-id"));
        boolean persistenceCommand = commandPath.size() == 3
                && "object-store".equals(commandPath.get(0))
                && "persistence".equals(commandPath.get(1));
        if (persistenceCommand) {
            if (runId.isEmpty()) {
                throw new IllegalArgumentException(
                        "--run-id is required for object-store persistence commands");
            }
            validateRunId(runId.orElseThrow());
        } else if (runId.isPresent()) {
            throw new IllegalArgumentException(
                    "--run-id is only valid for object-store persistence commands");
        }

        return new CommandLineArguments(
                commandPath, config, timeoutSeconds, output, runId);
    }

    private static int firstOptionOffset(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                return i;
            }
        }
        return args.length;
    }

    private static Map<String, String> parseOptions(String[] args, int offset) {
        Map<String, String> options = new LinkedHashMap<>();
        int i = offset;
        while (i < args.length) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException(
                        "unknown argument: " + arg + " (expected --option value)");
            }
            arg = arg.substring(2);
            if (arg.isEmpty()) {
                throw new IllegalArgumentException("empty option name");
            }
            if (!SUPPORTED_OPTIONS.contains(arg)) {
                throw new IllegalArgumentException("unknown option --" + arg);
            }
            i++;
            if (i >= args.length) {
                throw new IllegalArgumentException("missing value for option --" + arg);
            }
            String value = args[i];
            if (value.startsWith("--")) {
                throw new IllegalArgumentException("missing value for option --" + arg);
            }
            i++;
            if (options.containsKey(arg)) {
                throw new IllegalArgumentException("duplicate option --" + arg);
            }
            options.put(arg, value);
        }
        return options;
    }

    private static void validateRunId(String value) {
        if (value.length() < 26 || value.length() > 128) {
            throw new IllegalArgumentException(
                    "run-id must encode at least 128 bits and be at most 128 characters");
        }
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (!((current >= 'a' && current <= 'z')
                    || (current >= '2' && current <= '7'))) {
                throw new IllegalArgumentException(
                        "run-id must be lowercase base32 without padding");
            }
        }
    }

    @Override
    public String toString() {
        return "CommandLineArguments{commandPath=" + commandPath
                + ", config=" + config + ", timeoutSeconds=" + timeoutSeconds
                + ", outputFile=" + outputFile + ", runId=" + runId + "}";
    }
}
