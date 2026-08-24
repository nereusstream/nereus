/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.object.control;

/** Exact Cell-control metadata key grammar for the M3 WalRun record family. */
public final class WalRunControlKeys {
    private static final String PREFIX = "v2/object-wal/shards/";

    private WalRunControlKeys() {}

    public static String pointerKey(int shardId) {
        return shardPrefix(shardId) + "/current";
    }

    public static String rootKey(int shardId, long shardRunEpoch) {
        return runPrefix(shardId, shardRunEpoch) + "/root";
    }

    public static String sealKey(int shardId, long shardRunEpoch) {
        return runPrefix(shardId, shardRunEpoch) + "/seal";
    }

    public static String checkpointHeadKey(int shardId, long shardRunEpoch) {
        return runPrefix(shardId, shardRunEpoch) + "/checkpoint/head";
    }

    public static String checkpointPagePrefix(int shardId, long shardRunEpoch) {
        return runPrefix(shardId, shardRunEpoch) + "/checkpoint/pages";
    }

    public static String checkpointPageKey(
            int shardId, long shardRunEpoch, long pageOrdinal, com.nereusstream.domain.bytes.Sha256Digest pageSha256) {
        if (pageOrdinal < 0) {
            throw new IllegalArgumentException("checkpoint page ordinal must be non-negative");
        }
        return checkpointPagePrefix(shardId, shardRunEpoch) + "/" + fixedUnsigned(pageOrdinal, 20) + "-" + pageSha256;
    }

    public static void requirePointerKey(String supplied, int shardId) {
        requireExact(supplied, pointerKey(shardId), "CurrentWalRunPointer key");
    }

    public static void requireRootKey(String supplied, int shardId, long shardRunEpoch) {
        requireExact(supplied, rootKey(shardId, shardRunEpoch), "WalRun Root key");
    }

    public static void requireSealKey(String supplied, int shardId, long shardRunEpoch) {
        requireExact(supplied, sealKey(shardId, shardRunEpoch), "WalRun Seal key");
    }

    public static void requireCheckpointHeadKey(String supplied, int shardId, long shardRunEpoch) {
        requireExact(supplied, checkpointHeadKey(shardId, shardRunEpoch), "WalRun checkpoint-head key");
    }

    public static void requireCheckpointPagePrefix(String supplied, int shardId, long shardRunEpoch) {
        requireExact(supplied, checkpointPagePrefix(shardId, shardRunEpoch), "WalRun checkpoint-page prefix");
    }

    private static String runPrefix(int shardId, long shardRunEpoch) {
        if (shardRunEpoch < 0) {
            throw new IllegalArgumentException("shard run epoch must be non-negative");
        }
        return shardPrefix(shardId) + "/runs/" + fixedUnsigned(shardRunEpoch, 20);
    }

    private static String shardPrefix(int shardId) {
        if (shardId < 0) {
            throw new IllegalArgumentException("shard ID must be non-negative");
        }
        return PREFIX + fixedUnsigned(shardId, 10);
    }

    private static String fixedUnsigned(long value, int width) {
        return String.format(java.util.Locale.ROOT, "%0" + width + "d", value);
    }

    private static void requireExact(String supplied, String expected, String label) {
        if (!expected.equals(supplied)) {
            throw new IllegalArgumentException(label + " differs from the exact grammar: " + supplied);
        }
    }
}
