/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.profile;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ReadTargetType;
import java.util.Objects;
public record StorageExecutionPlan(StorageProfile profile, ReadTargetType primaryTargetType,
        ObjectPublicationMode publicationMode, DurabilityLevel allowedDurability) {
    public StorageExecutionPlan { profile = Objects.requireNonNull(profile).canonical();
        Objects.requireNonNull(primaryTargetType); Objects.requireNonNull(publicationMode);
        Objects.requireNonNull(allowedDurability); }
}
