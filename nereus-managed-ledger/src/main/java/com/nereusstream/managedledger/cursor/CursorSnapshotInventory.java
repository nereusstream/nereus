/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.VersionedCursorRetention;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorSnapshotReferenceRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Read-only F3 handoff that classifies listed snapshot objects against one
 * versioned retention/root scan. It never deletes bytes and is valid only
 * while every captured version and owner session still matches.
 */
public final class CursorSnapshotInventory {
    private final String cluster;
    private final CursorLedgerIdentity ledger;
    private final Authority authority;
    private final Map<String, RootVersion> roots;
    private final Map<ObjectKey, LiveReference> liveReferences;
    private final Set<ObjectKey> unreferencedCandidates;
    private final Set<ObjectKey> discoveredObjects;

    private CursorSnapshotInventory(
            String cluster,
            CursorLedgerIdentity ledger,
            Authority authority,
            Map<String, RootVersion> roots,
            Map<ObjectKey, LiveReference> liveReferences,
            Set<ObjectKey> unreferencedCandidates,
            Set<ObjectKey> discoveredObjects) {
        this.cluster = cluster;
        this.ledger = ledger;
        this.authority = authority;
        this.roots = roots;
        this.liveReferences = liveReferences;
        this.unreferencedCandidates = unreferencedCandidates;
        this.discoveredObjects = discoveredObjects;
    }

    public static CursorSnapshotInventory classify(
            String cluster,
            CursorLedgerIdentity ledger,
            VersionedCursorRetention retention,
            Collection<VersionedCursorState> cursorRoots,
            Collection<ObjectKey> discoveredObjects) {
        Objects.requireNonNull(ledger, "ledger");
        Objects.requireNonNull(retention, "retention");
        Objects.requireNonNull(cursorRoots, "cursorRoots");
        Objects.requireNonNull(discoveredObjects, "discoveredObjects");
        if (!retention.value().projection().equals(ledger.projection())) {
            throw new IllegalArgumentException("retention projection does not match snapshot inventory ledger");
        }

        List<VersionedCursorState> orderedRoots = new ArrayList<>(cursorRoots);
        orderedRoots.sort(Comparator.comparing(root -> root.value().cursorName()));
        LinkedHashMap<String, RootVersion> versions = new LinkedHashMap<>();
        LinkedHashMap<ObjectKey, LiveReference> references = new LinkedHashMap<>();
        for (VersionedCursorState versioned : orderedRoots) {
            CursorStateRecord root = Objects.requireNonNull(versioned, "cursorRoots contains null").value();
            validateRoot(ledger, root);
            Optional<ObjectKey> referenceKey = root.snapshotReference()
                    .map(CursorSnapshotReferenceRecord::objectKey)
                    .map(ObjectKey::new);
            RootVersion prior = versions.putIfAbsent(root.cursorName(), new RootVersion(
                    root.cursorGeneration(),
                    root.lifecycle(),
                    root.mutationSequence(),
                    versioned.metadataVersion(),
                    referenceKey));
            if (prior != null) {
                throw new IllegalArgumentException("snapshot inventory contains duplicate cursor roots");
            }
            if (root.lifecycle() == CursorRecordLifecycle.ACTIVE && root.snapshotReference().isPresent()) {
                CursorSnapshotReferenceRecord reference = root.snapshotReference().orElseThrow();
                ObjectKey key = new ObjectKey(reference.objectKey());
                if (!CursorSnapshotKeys.belongsToStream(cluster, ledger, key)) {
                    throw new IllegalArgumentException("live snapshot reference is outside its stream prefix");
                }
                LiveReference collision = references.putIfAbsent(key, new LiveReference(
                        root.cursorName(),
                        root.cursorGeneration(),
                        root.mutationSequence(),
                        versioned.metadataVersion(),
                        reference));
                if (collision != null) {
                    throw new IllegalArgumentException("two cursor roots reference the same snapshot object");
                }
            }
        }

        List<ObjectKey> orderedObjects = new ArrayList<>(discoveredObjects);
        orderedObjects.sort(Comparator.comparing(ObjectKey::value));
        LinkedHashSet<ObjectKey> discovered = new LinkedHashSet<>();
        for (ObjectKey key : orderedObjects) {
            Objects.requireNonNull(key, "discoveredObjects contains null");
            if (!CursorSnapshotKeys.belongsToStream(cluster, ledger, key)) {
                throw new IllegalArgumentException("discovered snapshot object is outside its stream prefix");
            }
            discovered.add(key);
        }
        if (!discovered.containsAll(references.keySet())) {
            throw new IllegalArgumentException("snapshot inventory listing omits a live root reference");
        }
        LinkedHashSet<ObjectKey> candidates = new LinkedHashSet<>(discovered);
        candidates.removeAll(references.keySet());
        Authority authority = new Authority(
                retention.metadataVersion(),
                retention.value().ownerSessionId(),
                retention.value().lifecycle(),
                retention.value().mutationSequence());
        return new CursorSnapshotInventory(
                new OxiaKeyspace(cluster).cluster(),
                ledger,
                authority,
                immutableMap(versions),
                immutableMap(references),
                immutableSet(candidates),
                immutableSet(discovered));
    }

    public CursorLedgerIdentity ledger() {
        return ledger;
    }

    public Authority authority() {
        return authority;
    }

    public Map<String, RootVersion> roots() {
        return roots;
    }

    public Map<ObjectKey, LiveReference> liveReferences() {
        return liveReferences;
    }

    public Set<ObjectKey> unreferencedCandidates() {
        return unreferencedCandidates;
    }

    public boolean deletionVetoed() {
        return authority.retentionLifecycle() != CursorRetentionLifecycle.ACTIVE;
    }

    public boolean stillMatches(
            VersionedCursorRetention retention,
            Collection<VersionedCursorState> cursorRoots) {
        try {
            CursorSnapshotInventory latest = classify(
                    cluster, ledger, retention, cursorRoots, discoveredObjects);
            return authority.equals(latest.authority)
                    && roots.equals(latest.roots)
                    && liveReferences.equals(latest.liveReferences);
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    private static void validateRoot(CursorLedgerIdentity ledger, CursorStateRecord root) {
        if (!root.projection().equals(ledger.projection())
                || !CursorNames.cursorNameHash(root.cursorName()).equals(root.cursorNameHash())) {
            throw new IllegalArgumentException("cursor root does not match snapshot inventory ledger/name");
        }
    }

    private static <K, V> Map<K, V> immutableMap(LinkedHashMap<K, V> source) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    private static <T> Set<T> immutableSet(LinkedHashSet<T> source) {
        return Collections.unmodifiableSet(new LinkedHashSet<>(source));
    }

    public record Authority(
            long retentionMetadataVersion,
            String ownerSessionId,
            CursorRetentionLifecycle retentionLifecycle,
            long retentionMutationSequence) {
        public Authority {
            if (retentionMetadataVersion < 0 || retentionMutationSequence < 1) {
                throw new IllegalArgumentException("snapshot inventory retention versions are invalid");
            }
            Objects.requireNonNull(ownerSessionId, "ownerSessionId");
            Objects.requireNonNull(retentionLifecycle, "retentionLifecycle");
        }
    }

    public record RootVersion(
            long cursorGeneration,
            CursorRecordLifecycle lifecycle,
            long mutationSequence,
            long metadataVersion,
            Optional<ObjectKey> snapshotKey) {
        public RootVersion {
            if (cursorGeneration < 1 || mutationSequence < 1 || metadataVersion < 0) {
                throw new IllegalArgumentException("snapshot inventory cursor versions are invalid");
            }
            Objects.requireNonNull(lifecycle, "lifecycle");
            snapshotKey = Objects.requireNonNull(snapshotKey, "snapshotKey");
        }
    }

    public record LiveReference(
            String cursorName,
            long cursorGeneration,
            long rootMutationSequence,
            long rootMetadataVersion,
            CursorSnapshotReferenceRecord reference) {
        public LiveReference {
            Objects.requireNonNull(cursorName, "cursorName");
            if (cursorGeneration < 1 || rootMutationSequence < 1 || rootMetadataVersion < 0) {
                throw new IllegalArgumentException("live snapshot reference versions are invalid");
            }
            Objects.requireNonNull(reference, "reference");
        }
    }
}
