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
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;

/**
 * Test-only Java agent that withholds one successful activation-store completion from the controller.
 *
 * <p>The real Oxia-backed store still applies and observes the PREPARED create or ACTIVE CAS. The substituted future never
 * completes successfully, creating a deterministic process-loss boundary after durable application but before the
 * controller activation coordinator observes completion.
 */
public final class ActivationCompletionGateAgent {
    private static final String PROPERTY_PREFIX =
            "nereus.f9.activation.completion.gate.";
    private static final String TARGET_TYPE =
            "com.nereusstream.metadata.oxia.OxiaJavaKafkaStorageActivationMetadataStore";

    private ActivationCompletionGateAgent() {
    }

    public static void premain(
            String agentArguments,
            Instrumentation instrumentation
    ) {
        Map<String, String> configuration =
                parseArguments(agentArguments);
        for (Map.Entry<String, String> entry :
                configuration.entrySet()) {
            System.setProperty(
                    PROPERTY_PREFIX + entry.getKey(),
                    entry.getValue());
        }
        String operation = require(configuration, "operation");
        require(configuration, "arm");
        require(configuration, "captured");
        require(configuration, "applied");
        require(configuration, "installed");

        ElementMatcher.Junction<MethodDescription> methodMatcher =
                switch (operation) {
                    case "createActivation" ->
                            named(operation).and(takesArguments(1));
                    case "compareAndSetActivation" ->
                            named(operation).and(takesArguments(2));
                    default ->
                            throw new IllegalArgumentException(
                                    "unsupported activation completion gate operation: "
                                            + operation);
                };
        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .ignore(nameStartsWith("net.bytebuddy."))
                .type(named(TARGET_TYPE))
                .transform(
                        (builder,
                                type,
                                classLoader,
                                module,
                                protectionDomain) ->
                                builder.visit(
                                        Advice.to(
                                                        ActivationCompletionAdvice.class)
                                                .on(methodMatcher)))
                .installOn(instrumentation);
        writeMarker(
                Path.of(configuration.get("installed")),
                "installed");
    }

    private static Map<String, String> parseArguments(
            String agentArguments
    ) {
        if (agentArguments == null || agentArguments.isBlank()) {
            throw new IllegalArgumentException(
                    "activation completion gate agent arguments are required");
        }
        Map<String, String> parsed = new LinkedHashMap<>();
        for (String token : agentArguments.split(",")) {
            int separator = token.indexOf('=');
            if (separator <= 0 || separator == token.length() - 1) {
                throw new IllegalArgumentException(
                        "invalid activation completion gate agent argument: "
                                + token);
            }
            String key = token.substring(0, separator);
            String value = token.substring(separator + 1);
            if (parsed.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException(
                        "duplicate activation completion gate agent argument: "
                                + key);
            }
        }
        return Map.copyOf(parsed);
    }

    private static String require(
            Map<String, String> configuration,
            String key
    ) {
        String value = configuration.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "missing activation completion gate agent argument: "
                            + key);
        }
        return value;
    }

    private static void writeMarker(
            Path marker,
            String value
    ) {
        try {
            Path parent = marker.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(marker, value);
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "cannot write activation completion gate marker "
                            + marker,
                    failure);
        }
    }

    public static final class ActivationCompletionAdvice {
        private ActivationCompletionAdvice() {
        }

        @Advice.OnMethodExit
        public static void gate(
                @Advice.Return(readOnly = false)
                        CompletableFuture<?> returned
        ) {
            Path arm = path("arm");
            if (!Files.exists(arm)) {
                return;
            }
            Path captured = path("captured");
            try {
                Path parent = captured.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.createFile(captured);
            } catch (FileAlreadyExistsException ignored) {
                return;
            } catch (IOException failure) {
                throw new IllegalStateException(
                        "cannot capture activation completion gate",
                        failure);
            }

            CompletableFuture<?> provider = returned;
            CompletableFuture<Object> delayed =
                    new CompletableFuture<>();
            returned = delayed;
            provider.whenComplete(
                    new ProviderCompletion(delayed));
        }

        public static Path path(String name) {
            String configured =
                    System.getProperty(PROPERTY_PREFIX + name);
            if (configured == null || configured.isBlank()) {
                throw new IllegalStateException(
                        "activation completion gate property is absent: "
                                + name);
            }
            return Path.of(configured);
        }
    }

    public static final class ProviderCompletion
            implements BiConsumer<Object, Throwable> {
        private final CompletableFuture<Object> delayed;

        public ProviderCompletion(
                CompletableFuture<Object> delayed
        ) {
            this.delayed = delayed;
        }

        @Override
        public void accept(
                Object value,
                Throwable failure
        ) {
            if (failure != null) {
                delayed.completeExceptionally(failure);
                return;
            }
            try {
                writeMarker(
                        ActivationCompletionAdvice.path("applied"),
                        System.getProperty(
                                PROPERTY_PREFIX + "operation"));
            } catch (Throwable markerFailure) {
                delayed.completeExceptionally(markerFailure);
            }
        }
    }
}
