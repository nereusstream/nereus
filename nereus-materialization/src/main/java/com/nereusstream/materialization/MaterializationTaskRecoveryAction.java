/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

/** Observable result of one idempotent durable-task recovery attempt. */
public enum MaterializationTaskRecoveryAction {
    NONE,
    DISPATCHED,
    EXPIRED_CLAIM_REQUEUED,
    PUBLICATION_RECONCILED
}
