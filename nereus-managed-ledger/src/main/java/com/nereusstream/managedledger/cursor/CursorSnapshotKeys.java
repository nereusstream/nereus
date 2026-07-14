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
}
