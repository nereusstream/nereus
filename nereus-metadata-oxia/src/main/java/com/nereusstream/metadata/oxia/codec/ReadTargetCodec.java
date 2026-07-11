/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.api.target.ReadTargetType;

/** Canonical payload codec for one durable target version. */
public interface ReadTargetCodec<T extends ReadTarget> {
    ReadTargetType targetType();
    int targetVersion();
    Class<T> targetClass();
    byte[] encode(T target);
    T decode(byte[] payload);
}
