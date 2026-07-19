/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import io.netty.buffer.ByteBuf;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.client.api.WriteFlag;

/** BookKeeper 4.18 public-client implementation. The borrowed client is deliberately never closed here. */
public final class DefaultBookKeeperClientOperations implements BookKeeperClientOperations {
    private final BookKeeper client;

    public DefaultBookKeeperClientOperations(BookKeeper borrowedClient) {
        this.client = Objects.requireNonNull(borrowedClient, "borrowedClient");
    }

    @Override
    public CompletableFuture<WriteAdvHandle> createAdvanced(
            long ledgerId,
            BookKeeperWalConfiguration configuration,
            byte[] password,
            Map<String, byte[]> customMetadata,
            BookKeeperOperationDeadline deadline) {
        if (!configuration.ledgerIdNamespace().contains(ledgerId)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("ledger id is outside reserved prefix"));
        }
        byte[] secret = Objects.requireNonNull(password, "password").clone();
        Map<String, byte[]> metadata = immutableMetadata(customMetadata);
        try {
            CompletableFuture<WriteAdvHandle> result = client.newCreateLedgerOp()
                    .withEnsembleSize(configuration.ensembleSize())
                    .withWriteQuorumSize(configuration.writeQuorumSize())
                    .withAckQuorumSize(configuration.ackQuorumSize())
                    .withDigestType(configuration.digestType().toClientType())
                    .withPassword(secret)
                    .withCustomMetadata(metadata)
                    .withWriteFlags(EnumSet.noneOf(WriteFlag.class))
                    .makeAdv()
                    .withLedgerId(ledgerId)
                    .execute();
            return mapped(deadline.bound(result), BookKeeperExceptionMapper.Operation.WRITE)
                    .whenComplete((ignored, failure) -> Arrays.fill(secret, (byte) 0));
        } catch (Throwable failure) {
            Arrays.fill(secret, (byte) 0);
            return CompletableFuture.failedFuture(BookKeeperExceptionMapper.map(
                    failure, BookKeeperExceptionMapper.Operation.WRITE));
        }
    }

    @Override
    public CompletableFuture<ReadHandle> open(
            long ledgerId, BookKeeperDigestType digestType, byte[] password, boolean recovery,
            BookKeeperOperationDeadline deadline) {
        byte[] secret = Objects.requireNonNull(password, "password").clone();
        try {
            CompletableFuture<ReadHandle> result = client.newOpenLedgerOp()
                    .withLedgerId(ledgerId)
                    .withRecovery(recovery)
                    .withDigestType(Objects.requireNonNull(digestType, "digestType").toClientType())
                    .withPassword(secret)
                    .execute();
            return mapped(deadline.bound(result), BookKeeperExceptionMapper.Operation.READ)
                    .whenComplete((ignored, failure) -> Arrays.fill(secret, (byte) 0));
        } catch (Throwable failure) {
            Arrays.fill(secret, (byte) 0);
            return CompletableFuture.failedFuture(BookKeeperExceptionMapper.map(
                    failure, BookKeeperExceptionMapper.Operation.READ));
        }
    }

    @Override public CompletableFuture<Long> write(
            WriteAdvHandle handle, long entryId, ByteBuf entry, BookKeeperOperationDeadline deadline) {
        return mapped(deadline.bound(handle.writeAsync(entryId, entry)), BookKeeperExceptionMapper.Operation.WRITE);
    }
    @Override public CompletableFuture<LedgerEntries> readUnconfirmed(
            ReadHandle handle, long first, long last, BookKeeperOperationDeadline deadline) {
        return mapped(deadline.bound(handle.readUnconfirmedAsync(first, last)),
                BookKeeperExceptionMapper.Operation.READ);
    }
    @Override public CompletableFuture<LedgerMetadata> metadata(
            long ledgerId, BookKeeperOperationDeadline deadline) {
        return mapped(deadline.bound(client.getLedgerMetadata(ledgerId)),
                BookKeeperExceptionMapper.Operation.METADATA);
    }
    @Override public CompletableFuture<Void> delete(long ledgerId, BookKeeperOperationDeadline deadline) {
        return mapped(deadline.bound(client.newDeleteLedgerOp().withLedgerId(ledgerId).execute()),
                BookKeeperExceptionMapper.Operation.DELETE);
    }

    private static Map<String, byte[]> immutableMetadata(Map<String, byte[]> source) {
        Objects.requireNonNull(source, "customMetadata");
        return source.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey, entry -> entry.getValue().clone()));
    }
    private static <T> CompletableFuture<T> mapped(
            CompletableFuture<T> source, BookKeeperExceptionMapper.Operation operation) {
        return source.handle((value, failure) -> {
            if (failure != null) throw new java.util.concurrent.CompletionException(
                    BookKeeperExceptionMapper.map(failure, operation));
            return value;
        });
    }
}
