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
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;

/**
 * Test-only Java agent that delays one BookKeeper write completion after the real provider future succeeds.
 *
 * <p>The target release process still executes the stock BookKeeper client write. The gate only withholds the
 * completed
 * future from the Nereus appender, creating a deterministic boundary where the entry is provider-applied while the
 * durable
 * reservation is still WRITING and no stream-head publication has started.
 */
public final class BookKeeperWriteCompletionGateAgent {
    private static final String PROPERTY_PREFIX = "nereus.f9.bookkeeper.write.gate.";

    private BookKeeperWriteCompletionGateAgent() {}

    public static void premain(String agentArguments, Instrumentation instrumentation) {
        Map<String, String> configuration = parseArguments(agentArguments);
        for (Map.Entry<String, String> entry : configuration.entrySet()) {
            System.setProperty(PROPERTY_PREFIX + entry.getKey(), entry.getValue());
        }
        require(configuration, "arm");
        require(configuration, "captured");
        require(configuration, "applied");
        require(configuration, "release");
        require(configuration, "installed");

        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .ignore(nameStartsWith("net.bytebuddy."))
                .type(named("com.nereusstream.bookkeeper.DefaultBookKeeperClientOperations"))
                .transform((builder, type, classLoader, module, protectionDomain) -> builder.visit(
                        Advice.to(WriteCompletionAdvice.class).on(named("write").and(takesArguments(4)))))
                .installOn(instrumentation);
        writeMarker(Path.of(configuration.get("installed")), "installed");
    }

    private static Map<String, String> parseArguments(String agentArguments) {
        if (agentArguments == null || agentArguments.isBlank()) {
            throw new IllegalArgumentException("BookKeeper write gate agent arguments are required");
        }
        Map<String, String> parsed = new LinkedHashMap<>();
        for (String token : agentArguments.split(",")) {
            int separator = token.indexOf('=');
            if (separator <= 0 || separator == token.length() - 1) {
                throw new IllegalArgumentException("invalid BookKeeper write gate agent argument: " + token);
            }
            String key = token.substring(0, separator);
            String value = token.substring(separator + 1);
            if (parsed.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException("duplicate BookKeeper write gate agent argument: " + key);
            }
        }
        return Map.copyOf(parsed);
    }

    private static void require(Map<String, String> configuration, String key) {
        if (!configuration.containsKey(key)) {
            throw new IllegalArgumentException("missing BookKeeper write gate agent argument: " + key);
        }
    }

    public static void writeMarker(Path marker, String value) {
        try {
            Path parent = marker.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(marker, value);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot write BookKeeper gate marker " + marker, failure);
        }
    }

    public static final class WriteCompletionAdvice {
        private WriteCompletionAdvice() {}

        @Advice.OnMethodExit
        public static void gate(
                @Advice.Argument(1) long requestedEntryId,
                @Advice.Return(readOnly = false) CompletableFuture<Long> returned) {
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
                throw new IllegalStateException("cannot capture BookKeeper write gate", failure);
            }

            CompletableFuture<Long> provider = returned;
            CompletableFuture<Long> delayed = new CompletableFuture<>();
            returned = delayed;
            provider.whenComplete(new ProviderCompletion(delayed, requestedEntryId));
        }

        public static final class ProviderCompletion implements BiConsumer<Long, Throwable> {
            private final CompletableFuture<Long> delayed;
            private final long requestedEntryId;

            public ProviderCompletion(CompletableFuture<Long> delayed, long requestedEntryId) {
                this.delayed = delayed;
                this.requestedEntryId = requestedEntryId;
            }

            @Override
            public void accept(Long writtenEntryId, Throwable failure) {
                if (failure != null) {
                    delayed.completeExceptionally(failure);
                    return;
                }
                if (writtenEntryId == null || writtenEntryId != requestedEntryId) {
                    delayed.completeExceptionally(
                            new IllegalStateException("BookKeeper write returned another entry id"));
                    return;
                }
                try {
                    writeMarker(path("applied"), Long.toString(writtenEntryId));
                } catch (Throwable markerFailure) {
                    delayed.completeExceptionally(markerFailure);
                    return;
                }
                Thread waiter = new Thread(new ReleaseWaiter(delayed, writtenEntryId), "f9-bookkeeper-write-gate");
                waiter.setDaemon(true);
                waiter.start();
            }
        }

        public static final class ReleaseWaiter implements Runnable {
            private final CompletableFuture<Long> delayed;
            private final long writtenEntryId;

            public ReleaseWaiter(CompletableFuture<Long> delayed, long writtenEntryId) {
                this.delayed = delayed;
                this.writtenEntryId = writtenEntryId;
            }

            @Override
            public void run() {
                Path release = path("release");
                while (!Files.exists(release)) {
                    LockSupport.parkNanos(10_000_000L);
                }
                delayed.complete(writtenEntryId);
            }
        }

        public static Path path(String name) {
            String configured = System.getProperty(PROPERTY_PREFIX + name);
            if (configured == null || configured.isBlank()) {
                throw new IllegalStateException("BookKeeper write gate property is absent: " + name);
            }
            return Path.of(configured);
        }
    }
}
