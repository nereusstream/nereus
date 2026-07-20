/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.profile;

import com.nereusstream.api.AppendCompletionPolicy;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ReadTargetType;
import java.util.function.Predicate;
public interface StorageProfileResolver {
    StorageExecutionPlan requireExecutable(StorageProfile profile, DurabilityLevel durability,
            boolean primaryAppenderInstalled, boolean primaryReaderInstalled);

    /** Resolves the exact producer-success predicate and rejects unsupported predicates before primary-WAL IO. */
    default StorageExecutionPlan requireExecutable(
            StorageProfile profile,
            DurabilityLevel durability,
            AppendCompletionPolicy completionPolicy,
            boolean primaryAppenderInstalled,
            boolean primaryReaderInstalled,
            boolean requiredObjectGenerationInstalled) {
        StorageExecutionPlan plan = requireExecutable(
                profile, durability, primaryAppenderInstalled, primaryReaderInstalled);
        AppendAckBoundary requested = switch (completionPolicy) {
            case PROFILE_DEFAULT -> plan.ackBoundary();
            case STABLE_HEAD -> AppendAckBoundary.STABLE_HEAD;
            case GENERATION_ZERO_INDEX -> AppendAckBoundary.GENERATION_ZERO_VISIBLE;
            case REQUIRED_OBJECT_GENERATION -> AppendAckBoundary.REQUIRED_OBJECT_GENERATION;
        };
        if (requested != plan.ackBoundary()) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_DURABILITY_LEVEL,
                    false,
                    "append completion policy weakens or changes the storage-profile success predicate",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        if (requested == AppendAckBoundary.REQUIRED_OBJECT_GENERATION
                && !requiredObjectGenerationInstalled) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "required Object-generation completion is not installed",
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        return plan;
    }

    /** Resolves the generation-zero reader required by a persisted profile and verifies it is installed. */
    ReadTargetType requireReadable(StorageProfile profile, Predicate<ReadTargetType> readerInstalled);
}
