/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/** Replayable compatibility upload backed by one immutable copied buffer. */
public final class ByteBufferObjectUpload implements ReplayableObjectUpload {
    private final byte[] bytes;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ByteBufferObjectUpload(ByteBuffer source) {
        ByteBuffer duplicate = Objects.requireNonNull(source, "source").asReadOnlyBuffer();
        this.bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
    }

    @Override
    public long contentLength() {
        return bytes.length;
    }

    @Override
    public Flow.Publisher<ByteBuffer> openPublisher() {
        if (closed.get()) {
            throw new IllegalStateException("upload source is closed");
        }
        return subscriber -> {
            Objects.requireNonNull(subscriber, "subscriber");
            subscriber.onSubscribe(new Flow.Subscription() {
                private boolean emitted;
                private boolean cancelled;

                @Override
                public synchronized void request(long count) {
                    if (cancelled || emitted) {
                        return;
                    }
                    if (count <= 0) {
                        emitted = true;
                        subscriber.onError(new IllegalArgumentException("publisher demand must be positive"));
                        return;
                    }
                    emitted = true;
                    if (bytes.length > 0) {
                        subscriber.onNext(ByteBuffer.wrap(bytes).asReadOnlyBuffer());
                    }
                    if (!cancelled) {
                        subscriber.onComplete();
                    }
                }

                @Override
                public synchronized void cancel() {
                    cancelled = true;
                }
            });
        };
    }

    @Override
    public void close() {
        closed.set(true);
    }
}
