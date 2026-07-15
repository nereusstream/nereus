/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.util.Objects;

/** Exact versioned protection value committed into a plan before conditional removal. */
public record GcPlannedProtectionRemoval(VersionedObjectProtection protection) {
    public GcPlannedProtectionRemoval {
        Objects.requireNonNull(protection, "protection");
    }

    public ObjectProtectionIdentity identity() {
        return new ObjectProtectionIdentity(
                new ObjectKeyHash(protection.value().objectKeyHash()),
                ObjectProtectionType.fromWireId(protection.value().protectionTypeId()),
                protection.value().referenceId());
    }
}
