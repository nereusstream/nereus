/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.util.Objects;

public record KafkaPartitionPendingOperationRecord(
        int operationTypeId,
        String attemptId,
        String ownerId,
        long ownerEpoch,
        long leaseExpiresAtMillis,
        long targetMetadataOffset,
        long startedAtMillis,
        String lastErrorCode) {
    public static final KafkaPartitionPendingOperationRecord EMPTY =
            new KafkaPartitionPendingOperationRecord(0, "", "", 0, 0, 0, 0, "");

    public KafkaPartitionPendingOperationRecord {
        KafkaPartitionOperationType type = KafkaPartitionOperationType.fromWireId(operationTypeId);
        Objects.requireNonNull(attemptId, "attemptId");
        Objects.requireNonNull(ownerId, "ownerId");
        lastErrorCode = KafkaMetadataValidation.bounded(lastErrorCode, "lastErrorCode");
        if (targetMetadataOffset < 0) throw new IllegalArgumentException("targetMetadataOffset must be non-negative");
        if (type == KafkaPartitionOperationType.NONE) {
            if (!attemptId.isEmpty() || !ownerId.isEmpty() || ownerEpoch != 0
                    || leaseExpiresAtMillis != 0 || startedAtMillis != 0 || !lastErrorCode.isEmpty()) {
                throw new IllegalArgumentException("NONE operation fields must be empty");
            }
        } else {
            KafkaMetadataValidation.text(attemptId, "attemptId");
            KafkaMetadataValidation.text(ownerId, "ownerId");
            if (ownerEpoch <= 0 || leaseExpiresAtMillis <= 0 || startedAtMillis <= 0) {
                throw new IllegalArgumentException("active operation term and time fields must be positive");
            }
        }
    }

    public KafkaPartitionOperationType operationType() {
        return KafkaPartitionOperationType.fromWireId(operationTypeId);
    }

    public boolean isEmpty() {
        return operationTypeId == 0;
    }
}
