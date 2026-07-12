/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.managedledger.cache.NereusNoopEntryCacheManager;
import com.nereusstream.managedledger.config.ManagedLedgerConfigValidator;
import com.nereusstream.managedledger.config.ManagedLedgerOpenConfigView;
import com.nereusstream.managedledger.errors.ManagedLedgerErrorMapper;
import com.nereusstream.managedledger.errors.OperationContext;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.managedledger.stats.NereusManagedLedgerFactoryStats;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenReadOnlyCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenReadOnlyManagedLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.util.DateFormatter;

/** F2 factory with exact-name single-flight opens and bounded handle ownership. */
public final class NereusManagedLedgerFactory implements ManagedLedgerFactory {
    private final NereusManagedLedgerRuntime runtime;
    private final ManagedLedgerConfig defaultConfig;
    private final ManagedLedgerFactoryConfig compatibilityConfig;
    private final boolean ownsRuntime;
    private final NereusManagedLedgerOpenCoordinator openCoordinator;
    private final ConcurrentMap<String, CompletableFuture<NereusManagedLedger>> ledgers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CompletableFuture<NereusReadOnlyManagedLedger>> readOnlyLedgers =
            new ConcurrentHashMap<>();
    private final Semaphore handlePermits;
    private final ManagedLedgerErrorMapper errorMapper = new ManagedLedgerErrorMapper();
    private final NereusNoopEntryCacheManager cacheManager = new NereusNoopEntryCacheManager();
    private final NereusManagedLedgerFactoryStats cacheStats;
    private final AtomicLong cacheEvictionThresholdNanos;
    private final AtomicBoolean closed = new AtomicBoolean();

    public NereusManagedLedgerFactory(
            NereusManagedLedgerRuntime runtime,
            NereusCreationGuard creationGuard,
            ManagedLedgerConfig defaultManagedLedgerConfig,
            ManagedLedgerFactoryConfig compatibilityFactoryConfig,
            boolean ownsRuntime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (runtime.isClosed()) {
            throw new IllegalArgumentException("runtime is already closed");
        }
        this.defaultConfig = Objects.requireNonNull(defaultManagedLedgerConfig, "defaultManagedLedgerConfig");
        ManagedLedgerConfigValidator.captureForOperation(defaultConfig);
        this.compatibilityConfig = Objects.requireNonNull(
                compatibilityFactoryConfig, "compatibilityFactoryConfig");
        this.compatibilityConfig.setMaxCacheSize(0);
        this.ownsRuntime = ownsRuntime;
        this.openCoordinator = new NereusManagedLedgerOpenCoordinator(
                runtime, Objects.requireNonNull(creationGuard, "creationGuard"));
        this.handlePermits = new Semaphore(runtime.config().maxOpenLedgers());
        this.cacheStats = new NereusManagedLedgerFactoryStats(this::completedLedgerCount);
        this.cacheEvictionThresholdNanos = new AtomicLong(saturatedMillisToNanos(
                compatibilityConfig.getCacheEvictionTimeThresholdMillis()));
    }

    public CompletableFuture<NereusStorageStateSnapshot> inspectStorageState(String managedLedgerName) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(factoryClosed());
        }
        return openCoordinator.inspectStorageState(managedLedgerName);
    }

    @Override
    public ManagedLedger open(String name) throws InterruptedException, ManagedLedgerException {
        return open(name, defaultConfig);
    }

    @Override
    public ManagedLedger open(String name, ManagedLedgerConfig config)
            throws InterruptedException, ManagedLedgerException {
        return await(openFuture(name, config));
    }

    @Override
    public void asyncOpen(String name, OpenLedgerCallback callback, Object ctx) {
        asyncOpen(name, defaultConfig, callback, () -> CompletableFuture.completedFuture(true), ctx);
    }

    @Override
    public void asyncOpen(
            String name,
            ManagedLedgerConfig config,
            OpenLedgerCallback callback,
            Supplier<CompletableFuture<Boolean>> mlOwnershipChecker,
            Object ctx) {
        Objects.requireNonNull(callback, "callback");
        Objects.requireNonNull(mlOwnershipChecker, "mlOwnershipChecker");
        openFuture(name, config).whenCompleteAsync((ledger, error) -> {
            if (error == null) {
                callback.openLedgerComplete(ledger, ctx);
            } else {
                callback.openLedgerFailed(mapFactory(error, "open"), ctx);
            }
        }, runtime.callbackExecutor());
    }

    private CompletableFuture<NereusManagedLedger> openFuture(String name, ManagedLedgerConfig config) {
        Objects.requireNonNull(name, "name");
        ManagedLedgerOpenConfigView configView;
        try {
            configView = ManagedLedgerConfigValidator.captureForOpen(config);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(factoryClosed());
        }
        CompletableFuture<NereusManagedLedger> existing = ledgers.get(name);
        if (existing != null) {
            return existing;
        }
        CompletableFuture<NereusManagedLedger> candidate = new CompletableFuture<>();
        existing = ledgers.putIfAbsent(name, candidate);
        if (existing != null) {
            return existing;
        }
        if (!handlePermits.tryAcquire()) {
            ledgers.remove(name, candidate);
            candidate.completeExceptionally(new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED, true, "managed-ledger handle capacity is exhausted"));
            return candidate;
        }
        openCoordinator.open(name, configView).whenComplete((opened, error) -> {
            if (error != null) {
                ledgers.remove(name, candidate);
                handlePermits.release();
                candidate.completeExceptionally(unwrap(error));
                return;
            }
            if (closed.get()) {
                ledgers.remove(name, candidate);
                handlePermits.release();
                candidate.completeExceptionally(factoryClosed());
                return;
            }
            try {
                NereusManagedLedger ledger = new NereusManagedLedger(
                        runtime,
                        opened,
                        config,
                        () -> releaseLedger(name, candidate));
                candidate.complete(ledger);
            } catch (Throwable constructionFailure) {
                ledgers.remove(name, candidate);
                handlePermits.release();
                candidate.completeExceptionally(constructionFailure);
            }
        });
        return candidate;
    }

    @Override
    public ReadOnlyCursor openReadOnlyCursor(
            String managedLedgerName, Position startPosition, ManagedLedgerConfig config)
            throws ManagedLedgerException {
        throw unsupported("openReadOnlyCursor");
    }

    @Override
    public void asyncOpenReadOnlyCursor(
            String managedLedgerName,
            Position startPosition,
            ManagedLedgerConfig config,
            OpenReadOnlyCursorCallback callback,
            Object ctx) {
        callback.openReadOnlyCursorFailed(unsupported("openReadOnlyCursor"), ctx);
    }

    @Override
    public void asyncOpenReadOnlyManagedLedger(
            String managedLedgerName,
            OpenReadOnlyManagedLedgerCallback callback,
            ManagedLedgerConfig config,
            Object ctx) {
        Objects.requireNonNull(callback, "callback");
        readOnlyFuture(managedLedgerName, config).whenCompleteAsync((ledger, error) -> {
            if (error == null) {
                callback.openReadOnlyManagedLedgerComplete(ledger, ctx);
            } else {
                callback.openReadOnlyManagedLedgerFailed(mapFactory(error, "openReadOnlyManagedLedger"), ctx);
            }
        }, runtime.callbackExecutor());
    }

    private CompletableFuture<NereusReadOnlyManagedLedger> readOnlyFuture(
            String name, ManagedLedgerConfig config) {
        ManagedLedgerOpenConfigView captured;
        try {
            captured = ManagedLedgerConfigValidator.captureForOpen(config);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(factoryClosed());
        }
        ManagedLedgerOpenConfigView noCreate = new ManagedLedgerOpenConfigView(
                captured.operationView(), false, Map.of());
        CompletableFuture<NereusReadOnlyManagedLedger> existing = readOnlyLedgers.get(name);
        if (existing != null) {
            return existing;
        }
        CompletableFuture<NereusReadOnlyManagedLedger> candidate = new CompletableFuture<>();
        existing = readOnlyLedgers.putIfAbsent(name, candidate);
        if (existing != null) {
            return existing;
        }
        if (!handlePermits.tryAcquire()) {
            readOnlyLedgers.remove(name, candidate);
            candidate.completeExceptionally(new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED, true, "managed-ledger handle capacity is exhausted"));
            return candidate;
        }
        openCoordinator.open(name, noCreate).whenComplete((opened, error) -> {
            if (error != null || closed.get()) {
                readOnlyLedgers.remove(name, candidate);
                handlePermits.release();
                candidate.completeExceptionally(error == null ? factoryClosed() : unwrap(error));
                return;
            }
            try {
                candidate.complete(new NereusReadOnlyManagedLedger(
                        runtime, opened, () -> releaseReadOnlyLedger(name, candidate)));
            } catch (Throwable constructionFailure) {
                readOnlyLedgers.remove(name, candidate);
                handlePermits.release();
                candidate.completeExceptionally(constructionFailure);
            }
        });
        return candidate;
    }

    @Override
    public ManagedLedgerInfo getManagedLedgerInfo(String name)
            throws InterruptedException, ManagedLedgerException {
        return await(infoFuture(name));
    }

    @Override
    public void asyncGetManagedLedgerInfo(String name, ManagedLedgerInfoCallback callback, Object ctx) {
        infoFuture(name).whenCompleteAsync((info, error) -> {
            if (error == null) {
                callback.getInfoComplete(info, ctx);
            } else {
                callback.getInfoFailed(mapFactory(error, "getManagedLedgerInfo"), ctx);
            }
        }, runtime.callbackExecutor());
    }

    private CompletableFuture<ManagedLedgerInfo> infoFuture(String name) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(factoryClosed());
        }
        return openCoordinator.inspectStorageState(name).thenCompose(snapshot -> {
            if (snapshot.state() != NereusDurableStorageState.ACTIVE
                    && snapshot.state() != NereusDurableStorageState.SEALED) {
                return notFound("managed ledger is not active or sealed");
            }
            return runtime.projectionStore().getProjection(runtime.cluster(), name)
                    .thenApply(optional -> toManagedLedgerInfo(
                            optional.orElseThrow(), snapshot.streamMetadata().orElseThrow()));
        });
    }

    @Override
    public void delete(String name) throws InterruptedException, ManagedLedgerException {
        delete(name, CompletableFuture.completedFuture(defaultConfig));
    }

    @Override
    public void delete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture)
            throws InterruptedException, ManagedLedgerException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        asyncDelete(name, mlConfigFuture, new DeleteLedgerCallback() {
            @Override public void deleteLedgerComplete(Object ctx) { result.complete(null); }
            @Override public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        await(result);
    }

    @Override
    public void asyncDelete(String name, DeleteLedgerCallback callback, Object ctx) {
        asyncDelete(name, CompletableFuture.completedFuture(defaultConfig), callback, ctx);
    }

    @Override
    public void asyncDelete(
            String name,
            CompletableFuture<ManagedLedgerConfig> mlConfigFuture,
            DeleteLedgerCallback callback,
            Object ctx) {
        Objects.requireNonNull(callback, "callback");
        CompletableFuture<NereusManagedLedger> open = ledgers.get(name);
        if (open != null) {
            open.whenComplete((ledger, error) -> {
                if (error == null) {
                    ledger.asyncDelete(callback, ctx);
                } else {
                    callback.deleteLedgerFailed(mapFactory(error, "delete"), ctx);
                }
            });
            return;
        }
        mlConfigFuture.thenCompose(config -> openExistingForDelete(name, config))
                .whenCompleteAsync((ignored, error) -> {
                    if (error == null) {
                        callback.deleteLedgerComplete(ctx);
                    } else {
                        callback.deleteLedgerFailed(mapFactory(error, "delete"), ctx);
                    }
                }, runtime.callbackExecutor());
    }

    private CompletableFuture<Void> openExistingForDelete(String name, ManagedLedgerConfig config) {
        ManagedLedgerOpenConfigView captured = ManagedLedgerConfigValidator.captureForOpen(config);
        if (!handlePermits.tryAcquire()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED, true, "managed-ledger handle capacity is exhausted"));
        }
        ManagedLedgerOpenConfigView noCreate = new ManagedLedgerOpenConfigView(
                captured.operationView(), false, Map.of());
        return openCoordinator.open(name, noCreate).thenCompose(opened -> {
            CompletableFuture<Void> result = new CompletableFuture<>();
            NereusManagedLedger ephemeral = new NereusManagedLedger(runtime, opened, config, () -> { });
            ephemeral.asyncDelete(new DeleteLedgerCallback() {
                @Override
                public void deleteLedgerComplete(Object ctx) {
                    try {
                        ephemeral.close();
                        result.complete(null);
                    } catch (ManagedLedgerException e) {
                        result.completeExceptionally(e);
                    }
                }

                @Override
                public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                    try {
                        ephemeral.close();
                    } catch (ManagedLedgerException closeFailure) {
                        exception.addSuppressed(closeFailure);
                    }
                    result.completeExceptionally(exception);
                }
            }, null);
            return result;
        }).whenComplete((ignored, error) -> handlePermits.release());
    }

    @Override
    public void shutdown() throws InterruptedException, ManagedLedgerException {
        await(shutdownAsync());
    }

    @Override
    public CompletableFuture<Void> shutdownAsync() {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> closes = new ArrayList<>();
        ledgers.values().forEach(open -> closes.add(open.handle((ledger, error) -> {
            if (ledger == null) {
                return CompletableFuture.<Void>completedFuture(null);
            }
            CompletableFuture<Void> closedLedger = new CompletableFuture<>();
            ledger.asyncClose(new org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback() {
                @Override public void closeComplete(Object ctx) { closedLedger.complete(null); }
                @Override public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    closedLedger.completeExceptionally(exception);
                }
            }, null);
            return closedLedger;
        }).thenCompose(value -> value)));
        readOnlyLedgers.values().forEach(open -> closes.add(open.handle((ledger, error) -> {
            if (ledger != null) {
                ledger.factoryClose();
            }
            return null;
        })));
        return CompletableFuture.allOf(closes.toArray(CompletableFuture[]::new))
                .thenRun(() -> {
                    ledgers.clear();
                    readOnlyLedgers.clear();
                    cacheManager.clear();
                })
                .thenCompose(ignored -> ownsRuntime
                        ? CompletableFuture.runAsync(runtime::close)
                        : CompletableFuture.completedFuture(null));
    }

    @Override
    public CompletableFuture<Boolean> asyncExists(String ledgerName) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(factoryClosed());
        }
        return openCoordinator.inspectStorageState(ledgerName)
                .thenApply(snapshot -> snapshot.state() == NereusDurableStorageState.ACTIVE
                        || snapshot.state() == NereusDurableStorageState.SEALED);
    }

    @Override public EntryCacheManager getEntryCacheManager() { return cacheManager; }
    @Override public void updateCacheEvictionTimeThreshold(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("cache eviction threshold cannot be negative");
        }
        cacheEvictionThresholdNanos.set(value);
    }
    @Override public long getCacheEvictionTimeThreshold() { return cacheEvictionThresholdNanos.get(); }
    @Override public void updateCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(int value) {
        cacheManager.updateCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(value);
    }
    @Override public void updateCacheEvictionExtendTTLOfRecentlyAccessed(boolean value) {
        cacheManager.updateCacheEvictionExtendTTLOfRecentlyAccessed(value);
    }

    @Override
    public CompletableFuture<Map<String, String>> getManagedLedgerPropertiesAsync(String name) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(factoryClosed());
        }
        return openCoordinator.inspectStorageState(name).thenCompose(snapshot -> {
            if (snapshot.state() != NereusDurableStorageState.ACTIVE
                    && snapshot.state() != NereusDurableStorageState.SEALED) {
                return notFound("managed ledger properties are unavailable");
            }
            return runtime.projectionStore().getProjection(runtime.cluster(), name)
                    .thenApply(optional -> Map.copyOf(optional.orElseThrow().properties()));
        });
    }

    @Override
    public Map<String, ManagedLedger> getManagedLedgers() {
        Map<String, ManagedLedger> result = new HashMap<>();
        ledgers.forEach((name, future) -> {
            NereusManagedLedger ledger = future.getNow(null);
            if (ledger != null) {
                result.put(name, ledger);
            }
        });
        return Map.copyOf(result);
    }

    @Override public ManagedLedgerFactoryMXBean getCacheStats() { return cacheStats; }

    @Override
    public void estimateUnloadedTopicBacklog(
            PersistentOfflineTopicStats offlineTopicStats,
            TopicName topicName,
            boolean accurate,
            Object ctx) throws Exception {
        Objects.requireNonNull(offlineTopicStats, "offlineTopicStats");
        Objects.requireNonNull(topicName, "topicName");
        String name = topicName.getPersistenceNamingEncoding();
        NereusStorageStateSnapshot snapshot = await(openCoordinator.inspectStorageState(name));
        if (snapshot.state() != NereusDurableStorageState.ACTIVE
                && snapshot.state() != NereusDurableStorageState.SEALED) {
            throw new ManagedLedgerException.ManagedLedgerNotFoundException(
                    "managed ledger is not active or sealed");
        }
        com.nereusstream.api.StreamMetadata metadata = snapshot.streamMetadata().orElseThrow();
        offlineTopicStats.reset();
        offlineTopicStats.totalMessages = metadata.committedEndOffset() - metadata.trimOffset();
        offlineTopicStats.storageSize = metadata.cumulativeSize();
        offlineTopicStats.messageBacklog = 0;
        if (accurate) {
            offlineTopicStats.addLedgerDetails(
                    offlineTopicStats.totalMessages,
                    snapshot.projection().orElseThrow().createdAtMillis(),
                    metadata.cumulativeSize(),
                    snapshot.projection().orElseThrow().virtualLedgerId());
        }
    }

    @Override public ManagedLedgerFactoryConfig getConfig() { return compatibilityConfig; }

    private void releaseLedger(String name, CompletableFuture<NereusManagedLedger> future) {
        if (ledgers.remove(name, future)) {
            cacheManager.removeEntryCache(name);
            handlePermits.release();
        }
    }

    private void releaseReadOnlyLedger(
            String name, CompletableFuture<NereusReadOnlyManagedLedger> future) {
        if (readOnlyLedgers.remove(name, future)) {
            handlePermits.release();
        }
    }

    private int completedLedgerCount() {
        int count = 0;
        for (CompletableFuture<NereusManagedLedger> future : ledgers.values()) {
            if (future.getNow(null) != null) {
                count++;
            }
        }
        for (CompletableFuture<NereusReadOnlyManagedLedger> future : readOnlyLedgers.values()) {
            if (future.getNow(null) != null) {
                count++;
            }
        }
        return count;
    }

    private ManagedLedgerException mapFactory(Throwable error, String operation) {
        return errorMapper.map(unwrap(error), new OperationContext(
                operation, true, false, java.util.Optional.empty()));
    }

    private ManagedLedgerException unsupported(String operation) {
        return errorMapper.unsupported(operation);
    }

    private static <T> CompletableFuture<T> notFound(String message) {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.STREAM_NOT_FOUND, false, message));
    }

    private ManagedLedgerException.ManagedLedgerFactoryClosedException factoryClosed() {
        return new ManagedLedgerException.ManagedLedgerFactoryClosedException(
                new IllegalStateException("Nereus managed-ledger factory is closed"));
    }

    private <T> T await(CompletableFuture<T> future)
            throws InterruptedException, ManagedLedgerException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw mapFactory(e, "synchronousFactoryOperation");
        }
    }

    private static ManagedLedgerInfo toManagedLedgerInfo(
            TopicProjectionRecord topic,
            com.nereusstream.api.StreamMetadata metadata) {
        ManagedLedgerInfo info = new ManagedLedgerInfo();
        info.version = topic.metadataVersion();
        info.creationDate = DateFormatter.format(topic.createdAtMillis());
        info.modificationDate = null;
        ManagedLedgerInfo.LedgerInfo ledger = new ManagedLedgerInfo.LedgerInfo();
        ledger.ledgerId = topic.virtualLedgerId();
        ledger.entries = metadata.committedEndOffset();
        ledger.size = metadata.cumulativeSize();
        ledger.timestamp = null;
        info.ledgers = List.of(ledger);
        info.cursors = Map.of();
        info.properties = topic.properties().isEmpty()
                ? null
                : new java.util.TreeMap<>(topic.properties());
        if (metadata.state() == com.nereusstream.api.StreamState.SEALED) {
            ManagedLedgerInfo.PositionInfo terminated = new ManagedLedgerInfo.PositionInfo();
            terminated.ledgerId = topic.virtualLedgerId();
            terminated.entryId = metadata.committedEndOffset() - 1;
            info.terminatedPosition = terminated;
        }
        return info;
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static long saturatedMillisToNanos(long millis) {
        if (millis <= 0) {
            return 0;
        }
        try {
            return Math.multiplyExact(millis, 1_000_000L);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }
}
