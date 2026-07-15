/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;

/** Store-level closed transition validator for the physical deletion authority root. */
final class PhysicalObjectRootTransitions {
    private PhysicalObjectRootTransitions() {
    }

    static void requireValidReplacement(
            PhysicalObjectRootRecord current,
            PhysicalObjectRootRecord replacement) {
        if (!sameImmutableRoot(current, replacement)) {
            throw invariant("physical root CAS attempted to change immutable identity");
        }
        PhysicalObjectLifecycle from = current.lifecycle();
        PhysicalObjectLifecycle to = replacement.lifecycle();
        if (from == to) {
            if (from != PhysicalObjectLifecycle.DELETED
                    || replacement.lifecycleEpoch() != current.lifecycleEpoch()
                    || !sameDeletedAuditBase(current, replacement)) {
                throw invariant("physical root same-state CAS is not a DELETED tombstone audit update");
            }
            return;
        }
        if (replacement.lifecycleEpoch() != Math.addExact(current.lifecycleEpoch(), 1)) {
            throw invariant("physical root lifecycle transition must increment epoch exactly once");
        }
        boolean allowed = switch (from) {
            case ACTIVE -> to == PhysicalObjectLifecycle.MARKED
                    || to == PhysicalObjectLifecycle.QUARANTINED;
            case MARKED -> to == PhysicalObjectLifecycle.ACTIVE
                    || to == PhysicalObjectLifecycle.DELETING
                    || to == PhysicalObjectLifecycle.QUARANTINED;
            case DELETING -> to == PhysicalObjectLifecycle.DELETED;
            case QUARANTINED -> to == PhysicalObjectLifecycle.MARKED;
            case DELETED -> false;
        };
        if (!allowed) {
            throw invariant("illegal physical root lifecycle transition: " + from + " -> " + to);
        }
    }

    static boolean sameImmutableIdentity(
            PhysicalObjectRootRecord left,
            PhysicalObjectRootRecord right) {
        return left.objectKeyHash().equals(right.objectKeyHash())
                && left.objectKey().equals(right.objectKey())
                && left.objectId().equals(right.objectId())
                && left.objectKindId() == right.objectKindId()
                && left.objectLength() == right.objectLength()
                && left.storageChecksumType().equals(right.storageChecksumType())
                && left.storageChecksumValue().equals(right.storageChecksumValue())
                && left.contentSha256().equals(right.contentSha256())
                && left.etag().equals(right.etag());
    }

    private static boolean sameImmutableRoot(
            PhysicalObjectRootRecord left,
            PhysicalObjectRootRecord right) {
        return sameImmutableIdentity(left, right)
                && left.createdAtMillis() == right.createdAtMillis()
                && left.orphanNotBeforeMillis() == right.orphanNotBeforeMillis();
    }

    private static boolean sameDeletedAuditBase(
            PhysicalObjectRootRecord left,
            PhysicalObjectRootRecord right) {
        return left.gcAttemptId().equals(right.gcAttemptId())
                && left.referenceSetSha256().equals(right.referenceSetSha256())
                && left.markedAtMillis() == right.markedAtMillis()
                && left.deleteNotBeforeMillis() == right.deleteNotBeforeMillis()
                && left.deleteStartedAtMillis() == right.deleteStartedAtMillis()
                && left.deletedAtMillis() == right.deletedAtMillis()
                && left.stateReason().equals(right.stateReason());
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
