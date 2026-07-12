/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

/** Product bootstrap boundary for deployable ObjectStore implementations. */
public interface ObjectStoreProvider extends AutoCloseable {
    ObjectStore create(
            ObjectStoreConfiguration configuration,
            ObjectStoreSecretResolver secretResolver) throws Exception;

    @Override
    void close();
}
