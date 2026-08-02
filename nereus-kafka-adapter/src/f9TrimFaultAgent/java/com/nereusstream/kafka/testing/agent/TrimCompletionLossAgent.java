/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.testing.agent;

import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;

/**
 * Test-only Java agent that loses one stream-trim completion after the real provider future succeeds.
 *
 * <p>The durable Oxia trim is allowed to finish. The future returned to the Kafka retention barrier is replaced by an
 * intentionally incomplete future, creating an exact provider-applied/caller-unobserved boundary for forced-process
 * recovery.
 */
public final class TrimCompletionLossAgent {
    private static final String PROPERTY_PREFIX = "nereus.f9.trim.completion.loss.";

    private TrimCompletionLossAgent() {}

    public static void premain(String agentArguments, Instrumentation instrumentation) {
        Map<String, String> configuration = parseArguments(agentArguments);
        for (Map.Entry<String, String> entry : configuration.entrySet()) {
            System.setProperty(PROPERTY_PREFIX + entry.getKey(), entry.getValue());
        }
        require(configuration, "target");
        require(configuration, "arm");
        require(configuration, "captured");
        require(configuration, "applied");
        require(configuration, "installed");
        try {
            long target = Long.parseLong(configuration.get("target"));
            if (target < 0) {
                throw new IllegalArgumentException("trim completion loss target must be non-negative");
            }
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("trim completion loss target must be a long", failure);
        }

        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .ignore(nameStartsWith("net.bytebuddy."))
                .type(named("com.nereusstream.core.DefaultStreamStorage"))
                .transform((builder, type, classLoader, module, protectionDomain) -> builder.visit(
                        Advice.to(TrimCompletionAdvice.class).on(named("trim").and(takesArguments(3)))))
                .installOn(instrumentation);
        writeMarker(Path.of(configuration.get("installed")), "installed");
    }

    private static Map<String, String> parseArguments(String agentArguments) {
        if (agentArguments == null || agentArguments.isBlank()) {
            throw new IllegalArgumentException("trim completion loss agent arguments are required");
        }
        Map<String, String> parsed = new LinkedHashMap<>();
        for (String token : agentArguments.split(",")) {
            int separator = token.indexOf('=');
            if (separator <= 0 || separator == token.length() - 1) {
                throw new IllegalArgumentException("invalid trim completion loss agent argument: " + token);
            }
            String key = token.substring(0, separator);
            String value = token.substring(separator + 1);
            if (parsed.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException("duplicate trim completion loss agent argument: " + key);
            }
        }
        return Map.copyOf(parsed);
    }

    private static String require(Map<String, String> configuration, String key) {
        String value = configuration.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing trim completion loss agent argument: " + key);
        }
        return value;
    }

    private static void writeMarker(Path marker, String value) {
        try {
            Path parent = marker.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(marker, value);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot write trim completion loss marker " + marker, failure);
        }
    }

    public static final class TrimCompletionAdvice {
        private TrimCompletionAdvice() {}

        @Advice.OnMethodExit
        public static void loseCompletion(
                @Advice.Argument(1) long beforeOffset,
                @Advice.Return(readOnly = false) CompletableFuture<Void> returned) {
            if (beforeOffset != target() || !Files.exists(path("arm")) || !capture()) {
                return;
            }
            CompletableFuture<Void> provider = returned;
            CompletableFuture<Void> lost = new CompletableFuture<>();
            returned = lost;
            provider.whenComplete(new ProviderCompletion(lost));
        }

        public static boolean capture() {
            Path captured = path("captured");
            try {
                Path parent = captured.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.createFile(captured);
                return true;
            } catch (FileAlreadyExistsException ignored) {
                return false;
            } catch (IOException failure) {
                throw new IllegalStateException("cannot capture trim completion loss", failure);
            }
        }

        public static long target() {
            return Long.parseLong(property("target"));
        }

        public static Path path(String name) {
            return Path.of(property(name));
        }

        public static String property(String name) {
            String configured = System.getProperty(PROPERTY_PREFIX + name);
            if (configured == null || configured.isBlank()) {
                throw new IllegalStateException("trim completion loss property is absent: " + name);
            }
            return configured;
        }
    }

    public static final class ProviderCompletion implements BiConsumer<Void, Throwable> {
        private final CompletableFuture<Void> caller;

        public ProviderCompletion(CompletableFuture<Void> caller) {
            this.caller = caller;
        }

        @Override
        public void accept(Void ignored, Throwable failure) {
            if (failure != null) {
                caller.completeExceptionally(failure);
                return;
            }
            try {
                writeMarker(TrimCompletionAdvice.path("applied"), Long.toString(TrimCompletionAdvice.target()));
            } catch (Throwable markerFailure) {
                caller.completeExceptionally(markerFailure);
            }
        }
    }
}
