/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.util.Optional;

/** Resolver for ambient-credential deployments; explicit secret references always fail closed. */
public final class NoopObjectStoreSecretResolver implements ObjectStoreSecretResolver {
    @Override public Optional<char[]> resolve(String secretReference) { return Optional.empty(); }
}
