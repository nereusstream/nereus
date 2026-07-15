/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.staging;

import java.nio.file.Path;

/** Package-private common identity for product staging and compaction spill files. */
interface ManagedStagingFile extends AutoCloseable {
    Path path();

    @Override
    void close();
}
