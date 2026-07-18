/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.core.physical.GcGlobalReferenceScope;
import com.nereusstream.managedledger.retention.ProjectionGenerationReferenceDomain;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.materialization.gc.RegisteredStreamGcGlobalReferenceScope;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import java.util.Objects;

/** One shared ownerless-reference interpretation used by both the activation guard and physical GC. */
public record Phase4GcReferenceDomainAssembly(
        GcGlobalReferenceScope globalScope,
        ProjectionGenerationReferenceDomain projectionDomain) {
    public Phase4GcReferenceDomainAssembly {
        Objects.requireNonNull(globalScope, "globalScope");
        Objects.requireNonNull(projectionDomain, "projectionDomain");
    }

    public static Phase4GcReferenceDomainAssembly create(
            String cluster,
            PhysicalGcConfig config,
            GenerationProtocolActivationStore activationStore,
            GenerationMetadataStore generationStore,
            ManagedLedgerProjectionMetadataStore projectionStore) {
        PhysicalGcConfig exactConfig = Objects.requireNonNull(config, "config");
        GcGlobalReferenceScope globalScope = new RegisteredStreamGcGlobalReferenceScope(
                cluster,
                Objects.requireNonNull(activationStore, "activationStore"),
                Objects.requireNonNull(generationStore, "generationStore"),
                NereusGenerationProtocolReferenceDomains.currentGcV1(),
                exactConfig.referenceDomainConfig());
        return new Phase4GcReferenceDomainAssembly(
                globalScope,
                new ProjectionGenerationReferenceDomain(
                        cluster,
                        Objects.requireNonNull(projectionStore, "projectionStore"),
                        exactConfig.referenceDomainConfig(),
                        globalScope));
    }
}
