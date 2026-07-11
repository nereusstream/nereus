/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.profile;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.StorageProfile;
public interface StorageProfileResolver {
    StorageExecutionPlan requireExecutable(StorageProfile profile, DurabilityLevel durability,
            boolean primaryAppenderInstalled, boolean primaryReaderInstalled);
}
