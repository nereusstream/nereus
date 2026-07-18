/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.metadata.oxia.CursorIds;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import java.util.Objects;

/** Canonical object-key layout shared by F3 snapshot persistence and the F4 inventory handoff. */
public final class CursorSnapshotKeys {
    private CursorSnapshotKeys() {
    }

    public static ObjectKeyPrefix clusterPrefix(String cluster) {
        String canonicalCluster = new OxiaKeyspace(cluster).cluster();
        return new ObjectKeyPrefix(KeyComponentCodec.encodeComponent(canonicalCluster)
                + "/cursor-snapshots/v1/");
    }

    public static String streamPrefix(String cluster, CursorLedgerIdentity ledger) {
        Objects.requireNonNull(ledger, "ledger");
        return clusterPrefix(cluster).value()
                + KeyComponentCodec.encodeComponent(ledger.projection().streamId())
                + "/";
    }

    public static ObjectKey objectKey(
            String cluster, CursorIdentity identity, String snapshotId) {
        Objects.requireNonNull(identity, "identity");
        String exactSnapshotId = CursorIds.requireRandomId(snapshotId, "snapshotId");
        return new ObjectKey(
                streamPrefix(cluster, identity.ledger())
                        + identity.cursorNameHash()
                        + "/"
                        + KeyComponentCodec.encodeNonNegativeLong(identity.cursorGeneration())
                        + "/"
                        + exactSnapshotId
                        + ".ncs");
    }

    public static boolean belongsToStream(
            String cluster, CursorLedgerIdentity ledger, ObjectKey key) {
        Objects.requireNonNull(key, "key");
        return key.value().startsWith(streamPrefix(cluster, ledger));
    }

    /** Strict inverse used by F4 inventory; prefix membership alone never authorizes deletion. */
    public static ParsedSnapshotKey parse(
            String cluster, CursorLedgerIdentity ledger, ObjectKey key) {
        Objects.requireNonNull(key, "key");
        ParsedOwnerlessSnapshotKey parsed = parseOwnerless(cluster, key);
        if (!parsed.streamId().value().equals(ledger.projection().streamId())) {
            throw new IllegalArgumentException("cursor snapshot key is outside its stream prefix");
        }
        return new ParsedSnapshotKey(
                parsed.cursorNameHash(),
                parsed.cursorGeneration(),
                parsed.snapshotId());
    }

    /** Strict cluster-wide inverse used by ownerless F4 object inventory. */
    public static ParsedOwnerlessSnapshotKey parseOwnerless(
            String cluster, ObjectKey key) {
        Objects.requireNonNull(key, "key");
        String prefix = clusterPrefix(cluster).value();
        if (!key.value().startsWith(prefix)) {
            throw new IllegalArgumentException("cursor snapshot key is outside the cluster prefix");
        }
        String[] components = key.value().substring(prefix.length()).split("/", -1);
        if (components.length != 4 || !isStableHash(components[1])) {
            throw new IllegalArgumentException("cursor snapshot key has a non-canonical owner path");
        }
        StreamId streamId = new StreamId(KeyComponentCodec.decodeComponent(components[0]));
        long cursorGeneration = KeyComponentCodec.decodeNonNegativeLong(components[2]);
        if (cursorGeneration < 1 || !components[3].endsWith(".ncs")) {
            throw new IllegalArgumentException("cursor snapshot key has an invalid generation/suffix");
        }
        String snapshotId = components[3].substring(
                0, components[3].length() - ".ncs".length());
        CursorIds.requireRandomId(snapshotId, "snapshotId");
        String canonical = prefix
                + KeyComponentCodec.encodeComponent(streamId.value())
                + "/"
                + components[1]
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(cursorGeneration)
                + "/"
                + snapshotId
                + ".ncs";
        if (!canonical.equals(key.value())) {
            throw new IllegalArgumentException("cursor snapshot key is not canonical");
        }
        return new ParsedOwnerlessSnapshotKey(
                streamId,
                components[1],
                cursorGeneration,
                snapshotId);
    }

    private static boolean isStableHash(String value) {
        if (value.length() != 52) {
            return false;
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'a' && character <= 'z')
                    || (character >= '2' && character <= '7'))) {
                return false;
            }
        }
        return true;
    }

    public record ParsedSnapshotKey(
            String cursorNameHash,
            long cursorGeneration,
            String snapshotId) {
        public ParsedSnapshotKey {
            if (!isStableHash(cursorNameHash) || cursorGeneration < 1) {
                throw new IllegalArgumentException("parsed cursor snapshot identity is invalid");
            }
            snapshotId = CursorIds.requireRandomId(snapshotId, "snapshotId");
        }
    }

    public record ParsedOwnerlessSnapshotKey(
            StreamId streamId,
            String cursorNameHash,
            long cursorGeneration,
            String snapshotId) {
        public ParsedOwnerlessSnapshotKey {
            Objects.requireNonNull(streamId, "streamId");
            if (!isStableHash(cursorNameHash) || cursorGeneration < 1) {
                throw new IllegalArgumentException("parsed ownerless cursor snapshot identity is invalid");
            }
            snapshotId = CursorIds.requireRandomId(snapshotId, "snapshotId");
        }
    }
}
