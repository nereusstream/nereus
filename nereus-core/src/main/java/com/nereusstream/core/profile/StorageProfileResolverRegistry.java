/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.profile;

import com.nereusstream.api.AppendCompletionPolicy;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ReadTargetType;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/** Immutable exact-profile registry used when one process installs more than one primary WAL. */
public final class StorageProfileResolverRegistry implements StorageProfileResolver {
    private final Map<StorageProfile, StorageProfileResolver> resolvers;

    public StorageProfileResolverRegistry(Map<StorageProfile, ? extends StorageProfileResolver> resolvers) {
        Objects.requireNonNull(resolvers, "resolvers");
        EnumMap<StorageProfile, StorageProfileResolver> installed = new EnumMap<>(StorageProfile.class);
        resolvers.forEach((rawProfile, rawResolver) -> {
            StorageProfile profile = Objects.requireNonNull(rawProfile, "profile").canonical();
            StorageProfileResolver resolver = Objects.requireNonNull(rawResolver, "resolver");
            if (installed.putIfAbsent(profile, resolver) != null) {
                throw new IllegalArgumentException("duplicate storage-profile resolver for " + profile);
            }
        });
        this.resolvers = Map.copyOf(installed);
    }

    public boolean supports(StorageProfile rawProfile) {
        StorageProfile profile = Objects.requireNonNull(rawProfile, "profile").canonical();
        return resolvers.containsKey(profile);
    }

    @Override
    public StorageExecutionPlan requireExecutable(
            StorageProfile profile,
            DurabilityLevel durability,
            boolean primaryAppenderInstalled,
            boolean primaryReaderInstalled) {
        return require(profile).requireExecutable(
                profile,
                durability,
                primaryAppenderInstalled,
                primaryReaderInstalled);
    }

    @Override
    public StorageExecutionPlan requireExecutable(
            StorageProfile profile,
            DurabilityLevel durability,
            AppendCompletionPolicy completionPolicy,
            boolean primaryAppenderInstalled,
            boolean primaryReaderInstalled,
            boolean requiredObjectGenerationInstalled) {
        return require(profile).requireExecutable(
                profile,
                durability,
                completionPolicy,
                primaryAppenderInstalled,
                primaryReaderInstalled,
                requiredObjectGenerationInstalled);
    }

    @Override
    public ReadTargetType requireReadable(
            StorageProfile profile,
            Predicate<ReadTargetType> readerInstalled) {
        return require(profile).requireReadable(profile, readerInstalled);
    }

    private StorageProfileResolver require(StorageProfile rawProfile) {
        StorageProfile profile = Objects.requireNonNull(rawProfile, "profile").canonical();
        StorageProfileResolver resolver = resolvers.get(profile);
        if (resolver == null) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "no storage-profile resolver is installed for " + profile,
                    AppendOutcome.KNOWN_NOT_COMMITTED);
        }
        return resolver;
    }
}
