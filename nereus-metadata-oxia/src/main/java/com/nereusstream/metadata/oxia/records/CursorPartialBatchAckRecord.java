/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.util.Arrays;
import java.util.Objects;

/** Canonical remaining-message bitset for one partially acknowledged persisted batch entry. */
public record CursorPartialBatchAckRecord(long entryOffset, int batchSize, long[] remainingWords) {
    public static final int MAX_BATCH_INDEXES = 131_072;

    public CursorPartialBatchAckRecord {
        if (entryOffset < 0) {
            throw new IllegalArgumentException("partial ack entryOffset must be non-negative");
        }
        if (batchSize <= 0 || batchSize > MAX_BATCH_INDEXES) {
            throw new IllegalArgumentException("partial ack batchSize is outside the F3 limit");
        }
        remainingWords = Objects.requireNonNull(remainingWords, "remainingWords").clone();
        int maximumWords = Math.addExact(batchSize, Long.SIZE - 1) / Long.SIZE;
        if (remainingWords.length == 0 || remainingWords.length > maximumWords) {
            throw new IllegalArgumentException("partial ack remainingWords has a noncanonical length");
        }
        if (remainingWords[remainingWords.length - 1] == 0) {
            throw new IllegalArgumentException("partial ack remainingWords contains trailing zero words");
        }
        int remainder = batchSize & (Long.SIZE - 1);
        if (remainder != 0 && remainingWords.length == maximumWords) {
            long allowedMask = (1L << remainder) - 1;
            if ((remainingWords[maximumWords - 1] & ~allowedMask) != 0) {
                throw new IllegalArgumentException("partial ack remainingWords sets a bit beyond batchSize");
            }
        }
        if (isAllRemaining(batchSize, remainingWords, maximumWords)) {
            throw new IllegalArgumentException("an all-remaining batch state is not durable partial ack state");
        }
    }

    @Override
    public long[] remainingWords() {
        return remainingWords.clone();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || (other instanceof CursorPartialBatchAckRecord that
                && entryOffset == that.entryOffset
                && batchSize == that.batchSize
                && Arrays.equals(remainingWords, that.remainingWords));
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(entryOffset, batchSize) + Arrays.hashCode(remainingWords);
    }

    private static boolean isAllRemaining(int batchSize, long[] words, int maximumWords) {
        if (words.length != maximumWords) {
            return false;
        }
        for (int index = 0; index < maximumWords - 1; index++) {
            if (words[index] != -1L) {
                return false;
            }
        }
        int remainder = batchSize & (Long.SIZE - 1);
        long finalMask = remainder == 0 ? -1L : (1L << remainder) - 1;
        return words[maximumWords - 1] == finalMask;
    }
}
