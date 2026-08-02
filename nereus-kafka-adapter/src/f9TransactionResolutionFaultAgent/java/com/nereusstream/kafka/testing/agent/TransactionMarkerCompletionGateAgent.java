/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.testing.agent;

import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.instrument.Instrumentation;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;

/**
 * Test-only Java agent that blocks one Kafka transaction marker append before or after durable provider completion.
 *
 * <p>The process fixture arms this agent only after the selected user partition has moved to the
 * cut broker and immediately before it asks the coordinator to abort the already-open
 * transaction. The first append for the configured topic is therefore the coordinator's abort
 * marker. In {@code before-provider} mode the real partition-storage append is skipped and its
 * caller observes an incomplete future. In {@code after-provider} mode the real append is allowed
 * to complete, an applied marker is persisted, and only the caller completion is lost. Killing the
 * release process at either boundary forces transaction-coordinator and data-log recovery to
 * resolve the prepared abort from durable truth.
 */
public final class TransactionMarkerCompletionGateAgent {
    private static final String PROPERTY_PREFIX = "nereus.f9.transaction.marker.completion.gate.";

    private TransactionMarkerCompletionGateAgent() {}

    public static void premain(String agentArguments, Instrumentation instrumentation) {
        Map<String, String> configuration = parseArguments(agentArguments);
        for (Map.Entry<String, String> entry : configuration.entrySet()) {
            System.setProperty(PROPERTY_PREFIX + entry.getKey(), entry.getValue());
        }
        String phase = require(configuration, "phase");
        if (!phase.equals("before-provider") && !phase.equals("after-provider")) {
            throw new IllegalArgumentException("unsupported transaction marker completion gate phase: " + phase);
        }
        require(configuration, "arm");
        require(configuration, "captured");
        require(configuration, "blocked");
        require(configuration, "applied");
        require(configuration, "failure");
        require(configuration, "installed");
        require(configuration, "topic");

        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .ignore(nameStartsWith("net.bytebuddy."))
                .type(named("com.nereusstream.kafka.partition.DefaultKafkaPartitionStorage"))
                .transform((builder, type, classLoader, module, protectionDomain) ->
                        builder.visit(Advice.to(TransactionMarkerAppendAdvice.class)
                                .on(named("append").and(takesArguments(2)))))
                .type(named("kafka.log.nereus.NereusUnifiedLog"))
                .transform((builder, type, classLoader, module, protectionDomain) ->
                        builder.visit(Advice.to(AppendFailureAdvice.class)
                                .on(named("appendAsLeader").and(takesArguments(7)))))
                .type(named("kafka.cluster.Partition"))
                .transform((builder, type, classLoader, module, protectionDomain) ->
                        builder.visit(Advice.to(AppendFailureAdvice.class)
                                .on(named("appendRecordsToLeader").and(takesArguments(6)))))
                .type(named("com.nereusstream.kafka.runtime.KafkaBoundedAppendExecutor"))
                .transform((builder, type, classLoader, module, protectionDomain) ->
                        builder.visit(Advice.to(ExecutorFailureAdvice.class)
                                .on(named("submit").and(takesArguments(3)))))
                .installOn(instrumentation);
        writeMarker(Path.of(configuration.get("installed")), "installed");
    }

    private static Map<String, String> parseArguments(String agentArguments) {
        if (agentArguments == null || agentArguments.isBlank()) {
            throw new IllegalArgumentException("transaction marker completion gate arguments are required");
        }
        Map<String, String> parsed = new LinkedHashMap<>();
        for (String token : agentArguments.split(",")) {
            int separator = token.indexOf('=');
            if (separator <= 0 || separator == token.length() - 1) {
                throw new IllegalArgumentException("invalid transaction marker completion gate argument: " + token);
            }
            String key = token.substring(0, separator);
            String value = token.substring(separator + 1);
            if (parsed.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException("duplicate transaction marker completion gate argument: " + key);
            }
        }
        return Map.copyOf(parsed);
    }

    private static String require(Map<String, String> configuration, String key) {
        String value = configuration.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing transaction marker completion gate argument: " + key);
        }
        return value;
    }

    public static void writeMarker(Path marker, String value) {
        try {
            Path parent = marker.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(marker, value);
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "cannot write transaction marker completion gate marker " + marker, failure);
        }
    }

    public static final class TransactionMarkerAppendAdvice {
        private TransactionMarkerAppendAdvice() {}

        @Advice.OnMethodEnter(skipOn = Advice.OnNonDefaultValue.class)
        public static boolean gateBeforeProvider(@Advice.Argument(1) Object context) {
            if (!property("phase").equals("before-provider") || !armedTargetAppend(context) || !capture()) {
                return false;
            }
            writeMarker(path("blocked"), "before-provider");
            return true;
        }

        @Advice.OnMethodExit
        public static void gateCompletion(
                @Advice.Enter boolean skipped,
                @Advice.Argument(1) Object context,
                @Advice.Return(readOnly = false) CompletableFuture<?> returned) {
            if (skipped) {
                returned = new CompletableFuture<>();
                return;
            }
            if (!property("phase").equals("after-provider") || !armedTargetAppend(context) || !capture()) {
                return;
            }
            CompletableFuture<?> provider = returned;
            CompletableFuture<Object> lost = new CompletableFuture<>();
            returned = lost;
            provider.whenComplete(new ProviderCompletion(lost));
        }

        public static boolean armedTargetAppend(Object context) {
            return Files.exists(path("arm")) && targetsTopic(context);
        }

        public static boolean targetsTopic(Object context) {
            try {
                Object tags = context.getClass().getMethod("tags").invoke(context);
                if (!(tags instanceof Map<?, ?> exactTags)) {
                    throw new IllegalStateException("Kafka append context tags are not a map: " + tags);
                }
                return property("topic").equals(exactTags.get("topic"));
            } catch (ReflectiveOperationException failure) {
                throw new IllegalStateException("cannot inspect Kafka append context topic", failure);
            }
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
                throw new IllegalStateException("cannot capture transaction marker completion gate", failure);
            }
        }

        public static Path path(String name) {
            return Path.of(property(name));
        }

        public static String property(String name) {
            String configured = System.getProperty(PROPERTY_PREFIX + name);
            if (configured == null || configured.isBlank()) {
                throw new IllegalStateException("transaction marker completion gate property is absent: " + name);
            }
            return configured;
        }
    }

    public static final class ProviderCompletion implements BiConsumer<Object, Throwable> {
        private final CompletableFuture<Object> caller;

        public ProviderCompletion(CompletableFuture<Object> caller) {
            this.caller = caller;
        }

        @Override
        public void accept(Object ignored, Throwable failure) {
            if (failure != null) {
                caller.completeExceptionally(failure);
                return;
            }
            try {
                writeMarker(TransactionMarkerAppendAdvice.path("applied"), "after-provider");
            } catch (Throwable markerFailure) {
                caller.completeExceptionally(markerFailure);
            }
        }
    }

    /**
     * Persists the first pre-provider exception so a missed cut is directly diagnosable.
     */
    public static final class AppendFailureAdvice {
        private AppendFailureAdvice() {}

        @Advice.OnMethodExit(onThrowable = Throwable.class)
        public static void captureFailure(@Advice.Thrown Throwable failure) {
            if (failure == null || !Files.exists(TransactionMarkerAppendAdvice.path("arm"))) {
                return;
            }
            Path marker = TransactionMarkerAppendAdvice.path("failure");
            try {
                Path parent = marker.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                StringWriter stack = new StringWriter();
                failure.printStackTrace(new PrintWriter(stack));
                Files.writeString(marker, stack.toString(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            } catch (FileAlreadyExistsException ignored) {
                // Preserve the first failure closest to the durable append seam.
            } catch (IOException markerFailure) {
                throw new IllegalStateException(
                        "cannot write transaction marker append failure evidence", markerFailure);
            }
        }
    }

    /**
     * Captures async rejection before the stock partition append callback starts.
     */
    public static final class ExecutorFailureAdvice {
        private ExecutorFailureAdvice() {}

        @Advice.OnMethodExit
        public static void captureFailure(@Advice.Return CompletableFuture<?> returned) {
            if (Files.exists(TransactionMarkerAppendAdvice.path("arm"))) {
                returned.whenComplete(new FailureCompletion());
            }
        }
    }

    public static final class FailureCompletion implements BiConsumer<Object, Throwable> {
        @Override
        public void accept(Object ignored, Throwable failure) {
            if (failure != null) {
                AppendFailureAdvice.captureFailure(failure);
            }
        }
    }
}
