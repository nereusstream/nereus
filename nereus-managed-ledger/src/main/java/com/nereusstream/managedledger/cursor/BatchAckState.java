/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Arrays;
import java.util.Objects;

/** Remaining-message bitset for one persisted Pulsar batch entry. */
public record BatchAckState(int batchSize, long[] remainingWords) {
    public BatchAckState {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        remainingWords = AckWords.canonicalCopy(batchSize, remainingWords);
    }

    @Override
    public long[] remainingWords() {
        return remainingWords.clone();
    }

    public boolean isWholeEntryAcknowledged() {
        return remainingWords.length == 0;
    }

    public boolean isAllRemaining() {
        return AckWords.isAllRemaining(batchSize, remainingWords);
    }

    public BatchAckState and(BatchAckState other) {
        Objects.requireNonNull(other, "other");
        if (batchSize != other.batchSize) {
            throw new IllegalArgumentException("cannot merge batch ack states with different batch sizes");
        }
        return new BatchAckState(batchSize, AckWords.and(batchSize, remainingWords, other.remainingWords));
    }

    @Override
    public boolean equals(Object other) {
        return this == other || (other instanceof BatchAckState that
                && batchSize == that.batchSize
                && Arrays.equals(remainingWords, that.remainingWords));
    }

    @Override
    public int hashCode() {
        return 31 * Integer.hashCode(batchSize) + Arrays.hashCode(remainingWords);
    }
}
