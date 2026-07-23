/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord;
import com.nereusstream.metadata.oxia.records.StreamHeadRecord;
import java.util.Objects;
import java.util.function.LongFunction;

/** Pure external-authority comparison and session transition shared by real and fake stores. */
public final class AppendAuthoritySessionTransitions {
    public static final String AUTHORITY_MODE_ATTRIBUTE = "nereus.append.authority.mode";
    public static final String EXTERNAL_MONOTONIC_TERM_V1 = "EXTERNAL_MONOTONIC_TERM_V1";

    private AppendAuthoritySessionTransitions() {
    }

    public static void requireLegacyMode(StreamHeadRecord head) {
        Objects.requireNonNull(head, "head");
        if (EXTERNAL_MONOTONIC_TERM_V1.equals(head.attributes().get(AUTHORITY_MODE_ATTRIBUTE))) {
            throw new NereusException(
                    ErrorCode.FENCED_APPEND,
                    false,
                    "legacy append-session acquisition is forbidden for an authority-bound stream");
        }
    }

    public static AppendSessionSnapshotRecord acquire(
            StreamHeadRecord head,
            AppendSessionOptions options,
            AppendAuthority requested,
            long nowMillis,
            long expiresAtMillis,
            LongFunction<String> tokenForEpoch) {
        Objects.requireNonNull(head, "head");
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(requested, "requested");
        Objects.requireNonNull(tokenForEpoch, "tokenForEpoch");
        if (!EXTERNAL_MONOTONIC_TERM_V1.equals(head.attributes().get(AUTHORITY_MODE_ATTRIBUTE))) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_APPEND_AUTHORITY,
                    false,
                    "stream is not configured for external monotonic append authority");
        }
        AppendSessionSnapshotRecord current = head.appendSession();
        if (current.isEmpty()) {
            return replacement(current, options, requested, expiresAtMillis, tokenForEpoch);
        }
        if (!current.hasAuthority()) {
            throw invariant("authority-bound stream contains a legacy append session");
        }
        if (!current.authorityType().equals(requested.authorityType())
                || !current.authorityId().equals(requested.authorityId())) {
            throw invariant("append authority identity changed for the same stream");
        }
        if (requested.authorityEpoch() > current.authorityEpoch()) {
            return replacement(current, options, requested, expiresAtMillis, tokenForEpoch);
        }
        if (requested.authorityEpoch() < current.authorityEpoch()) {
            throw fenced("append authority epoch is stale");
        }
        boolean sameOwner = current.authorityOwnerId().equals(requested.ownerId());
        if (sameOwner && requested.ownerEpoch() > current.authorityOwnerEpoch()) {
            return replacement(current, options, requested, expiresAtMillis, tokenForEpoch);
        }
        boolean exactAuthority = sameOwner
                && requested.ownerEpoch() == current.authorityOwnerEpoch();
        boolean live = current.expiresAtMillis() > nowMillis;
        if (exactAuthority && live && current.writerId().equals(options.writerId())) {
            return new AppendSessionSnapshotRecord(
                    current.writerId(),
                    current.epoch(),
                    current.fencingToken(),
                    Math.addExact(current.leaseVersion(), 1),
                    expiresAtMillis,
                    current.authorityType(),
                    current.authorityId(),
                    current.authorityEpoch(),
                    current.authorityOwnerId(),
                    current.authorityOwnerEpoch());
        }
        if (!sameOwner && requested.ownerEpoch() > current.authorityOwnerEpoch()) {
            throw fenced("owner epoch cannot preempt a different owner at the same authority epoch");
        }
        throw fenced("append authority does not dominate the active session");
    }

    public static AppendSessionSnapshotRecord preserveAuthorityOnRenewal(
            AppendSessionSnapshotRecord current,
            long expiresAtMillis) {
        Objects.requireNonNull(current, "current");
        return new AppendSessionSnapshotRecord(
                current.writerId(), current.epoch(), current.fencingToken(),
                Math.addExact(current.leaseVersion(), 1), expiresAtMillis,
                current.authorityType(), current.authorityId(), current.authorityEpoch(),
                current.authorityOwnerId(), current.authorityOwnerEpoch());
    }

    private static AppendSessionSnapshotRecord replacement(
            AppendSessionSnapshotRecord current,
            AppendSessionOptions options,
            AppendAuthority requested,
            long expiresAtMillis,
            LongFunction<String> tokenForEpoch) {
        long epoch = Math.addExact(current.epoch(), 1);
        return new AppendSessionSnapshotRecord(
                options.writerId(),
                epoch,
                tokenForEpoch.apply(epoch),
                Math.addExact(current.leaseVersion(), 1),
                expiresAtMillis,
                requested.authorityType(),
                requested.authorityId(),
                requested.authorityEpoch(),
                requested.ownerId(),
                requested.ownerEpoch());
    }

    private static NereusException fenced(String message) {
        return new NereusException(ErrorCode.FENCED_APPEND, false, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
