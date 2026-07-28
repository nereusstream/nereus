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
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;

/**
 * Test-only Java agent that blocks one first-activation boundary from the controller.
 *
 * <p>Before-provider mode skips the real proof/aggregation or Oxia publication call. After-provider mode lets the
 * operation finish successfully but withholds its completion from the controller. Both modes substitute a future that
 * never completes, creating deterministic process-loss boundaries on either side of the selected operation.
 */
public final class ActivationCompletionGateAgent {
    private static final String PROPERTY_PREFIX =
            "nereus.f9.activation.completion.gate.";
    private static final String ACTIVATION_STORE_TYPE =
            "com.nereusstream.metadata.oxia.OxiaJavaKafkaStorageActivationMetadataStore";
    private static final String ACTIVATION_COORDINATOR_TYPE =
            "com.nereusstream.kafka.activation.KafkaStorageFirstActivationCoordinator";

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
        String phase = require(configuration, "phase");
        if (!phase.equals("before-provider")
                && !phase.equals("after-provider")) {
            throw new IllegalArgumentException(
                    "unsupported activation completion gate phase: "
                            + phase);
        }
        require(configuration, "arm");
        require(configuration, "captured");
        require(configuration, "blocked");
        require(configuration, "applied");
        require(configuration, "installed");

        ElementMatcher.Junction<MethodDescription> methodMatcher =
                switch (operation) {
                    case "createReadiness",
                            "createActivation" ->
                            named(operation).and(takesArguments(1));
                    case "compareAndSetActivation" ->
                            named(operation).and(takesArguments(2));
                    case "currentSnapshot",
                            "loadCapabilities" ->
                            named(operation).and(takesArguments(
                                    operation.equals("currentSnapshot") ? 0 : 1));
                    default ->
                            throw new IllegalArgumentException(
                                    "unsupported activation completion gate operation: "
                                            + operation);
                };
        boolean proofOperation =
                operation.equals("currentSnapshot")
                        || operation.equals("loadCapabilities");
        boolean completionStageOperation =
                operation.equals("loadCapabilities");
        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .ignore(nameStartsWith("net.bytebuddy."))
                .type(named(
                        proofOperation
                                ? ACTIVATION_COORDINATOR_TYPE
                                : ACTIVATION_STORE_TYPE))
                .transform(
                        (builder,
                                type,
                                classLoader,
                                module,
                                protectionDomain) ->
                                builder.visit(
                                        Advice.to(
                                                        completionStageOperation
                                                                ? ActivationStageCompletionAdvice.class
                                                                : ActivationCompletionAdvice.class)
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

    public static void writeMarker(
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

        @Advice.OnMethodEnter(
                skipOn = Advice.OnNonDefaultValue.class)
        public static boolean gateBeforeProvider() {
            if (!property("phase")
                    .equals("before-provider")
                    || !Files.exists(path("arm"))
                    || !capture()) {
                return false;
            }
            writeMarker(
                    path("blocked"),
                    property("operation"));
            return true;
        }

        @Advice.OnMethodExit
        public static void gate(
                @Advice.Enter boolean skipped,
                @Advice.Return(readOnly = false)
                        CompletableFuture<?> returned
        ) {
            if (skipped) {
                returned = new CompletableFuture<>();
                return;
            }
            if (!property("phase")
                    .equals("after-provider")
                    || !Files.exists(path("arm"))) {
                return;
            }
            CompletableFuture<?> provider = returned;
            CompletableFuture<Object> delayed =
                    new CompletableFuture<>();
            returned = delayed;
            provider.whenComplete(
                    new ProviderCompletion(delayed));
        }

        public static boolean capture() {
            Path captured = path("captured");
            try {
                Path parent = captured.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.createFile(captured);
            } catch (FileAlreadyExistsException ignored) {
                return false;
            } catch (IOException failure) {
                throw new IllegalStateException(
                        "cannot capture activation completion gate",
                        failure);
            }
            return true;
        }

        public static Path path(String name) {
            return Path.of(property(name));
        }

        public static String property(String name) {
            String configured = System.getProperty(
                    PROPERTY_PREFIX + name);
            if (configured == null || configured.isBlank()) {
                throw new IllegalStateException(
                        "activation completion gate property is absent: "
                                + name);
            }
            return configured;
        }
    }

    public static final class ActivationStageCompletionAdvice {
        private ActivationStageCompletionAdvice() {
        }

        @Advice.OnMethodEnter(
                skipOn = Advice.OnNonDefaultValue.class)
        public static boolean gateBeforeProvider() {
            return ActivationCompletionAdvice.gateBeforeProvider();
        }

        @Advice.OnMethodExit
        public static void gate(
                @Advice.Enter boolean skipped,
                @Advice.Return(readOnly = false)
                        CompletionStage<?> returned
        ) {
            if (skipped) {
                returned = new CompletableFuture<>();
                return;
            }
            if (!ActivationCompletionAdvice.property("phase")
                    .equals("after-provider")
                    || !Files.exists(
                            ActivationCompletionAdvice.path("arm"))) {
                return;
            }
            CompletionStage<?> provider = returned;
            CompletableFuture<Object> delayed =
                    new CompletableFuture<>();
            returned = delayed;
            provider.whenComplete(
                    new ProviderCompletion(delayed));
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
            if (!ActivationCompletionAdvice.capture()) {
                delayed.complete(value);
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
