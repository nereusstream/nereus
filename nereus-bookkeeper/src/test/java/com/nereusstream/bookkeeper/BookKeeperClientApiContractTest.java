/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.CreateAdvBuilder;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.junit.jupiter.api.Test;

class BookKeeperClientApiContractTest {
    @Test
    void compilesAgainstPinnedPublic418Surface() throws Exception {
        assertThat(BookKeeper.class.getMethod("newCreateLedgerOp").getReturnType().getName())
                .isEqualTo("org.apache.bookkeeper.client.api.CreateBuilder");
        assertThat(CreateAdvBuilder.class.getMethod("withLedgerId", long.class)).isNotNull();
        assertThat(OpenBuilder.class.getMethod("withRecovery", boolean.class)).isNotNull();
        assertThat(DeleteBuilder.class.getMethods()).extracting(Method::getName).contains("withLedgerId");
        assertThat(WriteAdvHandle.class.getMethods()).extracting(Method::getName).contains("writeAsync");
        assertThat(ReadHandle.class.getMethods()).extracting(Method::getName).contains("readUnconfirmedAsync");
    }

    @Test
    void defaultAdapterPreservesTheCallerOwnedWriteBufferWhenBookKeeperConsumesItsArgument() {
        BookKeeper client = proxy(BookKeeper.class, (method, arguments) -> {
            throw new UnsupportedOperationException(method.getName());
        });
        WriteAdvHandle handle = proxy(WriteAdvHandle.class, (method, arguments) -> {
            if (method.getName().equals("writeAsync")) {
                ByteBuf transmitted = (ByteBuf) arguments[1];
                transmitted.release();
                return CompletableFuture.completedFuture((Long) arguments[0]);
            }
            throw new UnsupportedOperationException(method.getName());
        });
        ByteBuf callerOwned = Unpooled.wrappedBuffer(new byte[] {1, 2, 3});
        try {
            long written = new DefaultBookKeeperClientOperations(client)
                    .write(handle, 7, callerOwned, new BookKeeperOperationDeadline(Duration.ofSeconds(1)))
                    .join();

            assertThat(written).isEqualTo(7);
            assertThat(callerOwned.refCnt()).isOne();
            assertThat(callerOwned.readableBytes()).isEqualTo(3);
        } finally {
            callerOwned.release();
        }
    }

    @Test
    void defaultAdapterMakesDeferredSyncUnrepresentableAndAlwaysUsesEmptyWriteFlags() {
        AtomicReference<EnumSet<WriteFlag>> observedFlags = new AtomicReference<>();
        AtomicReference<CreateBuilder> createRef = new AtomicReference<>();
        AtomicReference<CreateAdvBuilder> advancedRef = new AtomicReference<>();
        WriteAdvHandle handle = proxy(WriteAdvHandle.class, (method, arguments) -> switch (method.getName()) {
            case "closeAsync" -> CompletableFuture.completedFuture(null);
            default -> throw new UnsupportedOperationException(method.getName());
        });
        CreateAdvBuilder advanced = proxy(CreateAdvBuilder.class, (method, arguments) -> switch (method.getName()) {
            case "withLedgerId" -> advancedRef.get();
            case "execute" -> CompletableFuture.completedFuture(handle);
            default -> throw new UnsupportedOperationException(method.getName());
        });
        advancedRef.set(advanced);
        CreateBuilder create = proxy(CreateBuilder.class, (method, arguments) -> switch (method.getName()) {
            case "withWriteFlags" -> {
                @SuppressWarnings("unchecked")
                EnumSet<WriteFlag> flags = (EnumSet<WriteFlag>) arguments[0];
                observedFlags.set(EnumSet.copyOf(flags));
                yield createRef.get();
            }
            case "makeAdv" -> advanced;
            case "withEnsembleSize", "withWriteQuorumSize", "withAckQuorumSize", "withPassword",
                    "withCustomMetadata", "withDigestType" -> createRef.get();
            default -> throw new UnsupportedOperationException(method.getName());
        });
        createRef.set(create);
        BookKeeper client = proxy(BookKeeper.class, (method, arguments) -> {
            if (method.getName().equals("newCreateLedgerOp")) return create;
            throw new UnsupportedOperationException(method.getName());
        });
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        long ledgerId = configuration.ledgerIdNamespace().candidate(new Random(7));

        WriteAdvHandle created = new DefaultBookKeeperClientOperations(client)
                .createAdvanced(
                        ledgerId,
                        configuration,
                        new byte[] {1},
                        Map.of(),
                        new BookKeeperOperationDeadline(Duration.ofSeconds(1)))
                .join();

        assertThat(created).isSameAs(handle);
        assertThat(observedFlags).hasValueSatisfying(flags -> assertThat(flags).isEmpty());
        assertThat(BookKeeperWalConfiguration.class.getRecordComponents())
                .extracting(component -> component.getName().toLowerCase(java.util.Locale.ROOT))
                .noneMatch(name -> name.contains("writeflag") || name.contains("deferredsync"));
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "toString" -> type.getSimpleName() + "Proxy";
                    case "hashCode" -> System.identityHashCode(ignored);
                    case "equals" -> ignored == arguments[0];
                    default -> invocation.invoke(method, arguments == null ? new Object[0] : arguments);
                });
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(Method method, Object[] arguments) throws Throwable;
    }
}
