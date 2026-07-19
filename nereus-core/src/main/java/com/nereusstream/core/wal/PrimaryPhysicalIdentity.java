/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal;

import com.nereusstream.api.target.ReadTargetType;

/** Exact provider identity carried through common append/protection code without provider casts. */
public interface PrimaryPhysicalIdentity {
    ReadTargetType targetType();
    byte[] canonicalIdentity();
}
