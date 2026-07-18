/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.metadata.oxia.CursorIds;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import java.util.Objects;

/** Canonical object-key layout shared by F3 snapshot persistence and the F4 inventory handoff. */
public final class CursorSnapshotKeys {
    private CursorSnapshotKeys() {
    }

    public static String streamPrefix(String cluster, CursorLedgerIdentity ledger) {
        Objects.requireNonNull(ledger, "ledger");
        String canonicalCluster = new OxiaKeyspace(cluster).cluster();
        return KeyComponentCodec.encodeComponent(canonicalCluster)
                + "/cursor-snapshots/v1/"
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
        String prefix = streamPrefix(cluster, ledger);
        if (!key.value().startsWith(prefix)) {
            throw new IllegalArgumentException("cursor snapshot key is outside its stream prefix");
        }
        String[] components = key.value().substring(prefix.length()).split("/", -1);
        if (components.length != 3 || !isStableHash(components[0])) {
            throw new IllegalArgumentException("cursor snapshot key has a non-canonical cursor hash/path");
        }
        long cursorGeneration = KeyComponentCodec.decodeNonNegativeLong(components[1]);
        if (cursorGeneration < 1 || !components[2].endsWith(".ncs")) {
            throw new IllegalArgumentException("cursor snapshot key has an invalid generation/suffix");
        }
        String snapshotId = components[2].substring(
                0, components[2].length() - ".ncs".length());
        CursorIds.requireRandomId(snapshotId, "snapshotId");
        String canonical = prefix
                + components[0]
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(cursorGeneration)
                + "/"
                + snapshotId
                + ".ncs";
        if (!canonical.equals(key.value())) {
            throw new IllegalArgumentException("cursor snapshot key is not canonical");
        }
        return new ParsedSnapshotKey(
                components[0], cursorGeneration, snapshotId);
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
}
