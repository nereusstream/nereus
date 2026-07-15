/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.nio.ByteBuffer;
import java.util.concurrent.Flow;

/** Close-owned immutable upload source that opens a fresh publisher for each admitted attempt. */
public interface ReplayableObjectUpload extends AutoCloseable {
    long contentLength();

    Flow.Publisher<ByteBuffer> openPublisher();

    @Override
    void close();
}
