/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.junit.jupiter.api.Test;

class BookKeeperLedgerHandleCacheTest {
    @Test
    void releasedLeastRecentlyUsedHandleIsEvictedBeforeBackpressure() {
        AtomicBoolean firstClosed = new AtomicBoolean();
        AtomicBoolean secondClosed = new AtomicBoolean();
        AtomicBoolean thirdClosed = new AtomicBoolean();
        ReadHandle first = readHandle(1, firstClosed);
        ReadHandle second = readHandle(2, secondClosed);
        ReadHandle third = readHandle(3, thirdClosed);
        try (BookKeeperLedgerHandleCache cache =
                new BookKeeperLedgerHandleCache(
                        2,
                        2,
                        1,
                        Duration.ofHours(1))) {
            BookKeeperLedgerHandleCache.Lease firstLease =
                    cache.borrow(key(1), () -> CompletableFuture.completedFuture(first)).join();
            firstLease.close();
            BookKeeperLedgerHandleCache.Lease secondLease =
                    cache.borrow(key(2), () -> CompletableFuture.completedFuture(second)).join();

            BookKeeperLedgerHandleCache.Lease thirdLease =
                    cache.borrow(key(3), () -> CompletableFuture.completedFuture(third)).join();

            assertThat(firstClosed).isTrue();
            assertThat(secondClosed).isFalse();
            assertThat(thirdClosed).isFalse();
            secondLease.close();
            thirdLease.close();
        }
        assertThat(secondClosed).isTrue();
        assertThat(thirdClosed).isTrue();
    }

    @Test
    void referencedHandleStillEnforcesTheCapacityBound() {
        AtomicBoolean firstClosed = new AtomicBoolean();
        try (BookKeeperLedgerHandleCache cache =
                new BookKeeperLedgerHandleCache(
                        1,
                        1,
                        1,
                        Duration.ofHours(1))) {
            BookKeeperLedgerHandleCache.Lease firstLease =
                    cache.borrow(
                                    key(1),
                                    () -> CompletableFuture.completedFuture(
                                            readHandle(1, firstClosed)))
                            .join();

            assertThatThrownBy(
                            () ->
                                    cache.borrow(
                                                    key(2),
                                                    () -> CompletableFuture.completedFuture(
                                                            readHandle(
                                                                    2,
                                                                    new AtomicBoolean())))
                                            .join())
                    .hasCauseInstanceOf(NereusException.class)
                    .satisfies(
                            failure ->
                                    assertThat(
                                                    ((NereusException) failure.getCause())
                                                            .code())
                                            .isEqualTo(
                                                    ErrorCode.BACKPRESSURE_REJECTED));
            assertThat(firstClosed).isFalse();
            firstLease.close();
        }
        assertThat(firstClosed).isTrue();
    }

    private static BookKeeperLedgerHandleCache.Key key(long ledgerId) {
        return new BookKeeperLedgerHandleCache.Key("cluster", ledgerId, 1);
    }

    private static ReadHandle readHandle(
            long ledgerId,
            AtomicBoolean closed
    ) {
        return (ReadHandle)
                Proxy.newProxyInstance(
                        ReadHandle.class.getClassLoader(),
                        new Class<?>[] {ReadHandle.class},
                        (proxy, method, arguments) ->
                                switch (method.getName()) {
                                    case "getId" -> ledgerId;
                                    case "closeAsync" -> {
                                        closed.set(true);
                                        yield CompletableFuture.completedFuture(null);
                                    }
                                    case "isClosed" -> closed.get();
                                    case "toString" -> "cache-read-handle-" + ledgerId;
                                    default -> defaultValue(method.getReturnType());
                                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        throw new IllegalArgumentException("unsupported primitive type");
    }
}
