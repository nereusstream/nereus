/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.target.ReadTargetType;

/**
 * Provider-neutral durable proof consumed at a metadata visibility cut.
 *
 * <p>The interface is intentionally extensible because provider proof values live in separate modules. Metadata
 * implementations must still explicitly admit and validate every concrete proof type before advancing visibility.
 */
public interface PhysicalReferenceProof {
    PhysicalReferencePurpose purpose();
    ReadTargetType targetType();
    Checksum targetIdentitySha256();
    String referenceId();
}
