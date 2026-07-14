/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

/** Canonical remaining-message bitset operations for persisted Pulsar batches. */
public final class AckWords {
    private AckWords() {
    }

    public static long[] canonicalCopy(int batchSize, long[] words) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        Objects.requireNonNull(words, "words");
        int maximumWords = Math.addExact(batchSize, Long.SIZE - 1) / Long.SIZE;
        if (words.length > maximumWords) {
            throw new IllegalArgumentException("remaining ack words exceed batchSize");
        }
        long[] copy = words.clone();
        int remainder = batchSize & (Long.SIZE - 1);
        if (remainder != 0 && copy.length == maximumWords) {
            long allowedMask = (1L << remainder) - 1;
            if ((copy[maximumWords - 1] & ~allowedMask) != 0) {
                throw new IllegalArgumentException("remaining ack words set a bit beyond batchSize");
            }
        }
        return BitSet.valueOf(copy).toLongArray();
    }

    public static boolean isAllRemaining(int batchSize, long[] canonicalWords) {
        Objects.requireNonNull(canonicalWords, "canonicalWords");
        int wordCount = Math.addExact(batchSize, Long.SIZE - 1) / Long.SIZE;
        if (canonicalWords.length != wordCount) {
            return false;
        }
        for (int index = 0; index < wordCount - 1; index++) {
            if (canonicalWords[index] != -1L) {
                return false;
            }
        }
        int remainder = batchSize & (Long.SIZE - 1);
        long finalMask = remainder == 0 ? -1L : (1L << remainder) - 1;
        return canonicalWords[wordCount - 1] == finalMask;
    }

    public static long[] and(int batchSize, long[] left, long[] right) {
        long[] canonicalLeft = canonicalCopy(batchSize, left);
        long[] canonicalRight = canonicalCopy(batchSize, right);
        int length = Math.max(canonicalLeft.length, canonicalRight.length);
        long[] merged = new long[length];
        for (int index = 0; index < length; index++) {
            long leftWord = index < canonicalLeft.length ? canonicalLeft[index] : 0;
            long rightWord = index < canonicalRight.length ? canonicalRight[index] : 0;
            merged[index] = leftWord & rightWord;
        }
        return canonicalCopy(batchSize, merged);
    }

    static boolean equal(long[] left, long[] right) {
        return Arrays.equals(left, right);
    }
}
