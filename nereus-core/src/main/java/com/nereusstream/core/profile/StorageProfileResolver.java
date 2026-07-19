/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.profile;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ReadTargetType;
import java.util.function.Predicate;
public interface StorageProfileResolver {
    StorageExecutionPlan requireExecutable(StorageProfile profile, DurabilityLevel durability,
            boolean primaryAppenderInstalled, boolean primaryReaderInstalled);

    /** Resolves the generation-zero reader required by a persisted profile and verifies it is installed. */
    ReadTargetType requireReadable(StorageProfile profile, Predicate<ReadTargetType> readerInstalled);
}
