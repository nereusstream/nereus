/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.util.Optional;

@FunctionalInterface
public interface ObjectStoreSecretResolver {
    Optional<char[]> resolve(String secretReference);
}
