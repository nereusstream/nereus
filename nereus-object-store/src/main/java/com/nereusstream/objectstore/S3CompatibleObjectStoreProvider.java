/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

/** Deployable AWS SDK v2 async provider for AWS S3 and path-style compatible services. */
public final class S3CompatibleObjectStoreProvider implements ObjectStoreProvider {
    private final AtomicBoolean used = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile S3CompatibleObjectStore store;
    private volatile AutoCloseable credentialsResource;

    @Override
    public ObjectStore create(ObjectStoreConfiguration config, ObjectStoreSecretResolver resolver) throws Exception {
        if (!used.compareAndSet(false, true)) {
            throw new IllegalStateException("S3 provider creates exactly one store");
        }
        AwsCredentialsProvider credentials = credentials(config, resolver);
        credentialsResource = credentials instanceof AutoCloseable closeable ? closeable : null;
        NettyNioAsyncHttpClient.Builder http = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(config.maxConnections())
                .connectionTimeout(config.requestTimeout())
                .readTimeout(config.requestTimeout())
                .writeTimeout(config.requestTimeout());
        S3AsyncClient client = null;
        ScheduledExecutorService deadlineScheduler = null;
        try {
            client = S3AsyncClient.builder()
                    .httpClientBuilder(http)
                    .credentialsProvider(credentials)
                    .endpointOverride(config.endpoint())
                    .region(Region.of(config.region()))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(config.pathStyleAccess())
                            .build())
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .apiCallTimeout(config.requestTimeout())
                            .apiCallAttemptTimeout(config.requestTimeout())
                            .build())
                    .build();
            deadlineScheduler = Executors.newSingleThreadScheduledExecutor(
                    daemonFactory("nereus-s3-deadline"));
            verifyBucket(client, config);
            store = new S3CompatibleObjectStore(client, deadlineScheduler, config.bucket(), config.prefix());
            return store;
        } catch (Throwable failure) {
            if (deadlineScheduler != null) {
                deadlineScheduler.shutdownNow();
            }
            if (client != null) {
                client.close();
            }
            closeCredentials();
            if (failure instanceof Exception exception) {
                throw exception;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("unexpected S3 provider bootstrap failure");
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        S3CompatibleObjectStore value = store;
        if (value != null) {
            value.close();
        }
        closeCredentials();
    }

    private static void verifyBucket(S3AsyncClient client, ObjectStoreConfiguration config) throws Exception {
        CompletableFuture<?> request = client.headBucket(HeadBucketRequest.builder().bucket(config.bucket()).build());
        try {
            request.get(config.requestTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable failure) {
            request.cancel(true);
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw S3ObjectErrorMapper.bootstrap(failure, config.bucket());
        }
    }

    static AwsCredentialsProvider credentials(
            ObjectStoreConfiguration config,
            ObjectStoreSecretResolver resolver) {
        if (config.accessKeySecretRef().isEmpty()) {
            return DefaultCredentialsProvider.builder().build();
        }
        char[] access = null;
        char[] secret = null;
        char[] token = null;
        try {
            access = resolve(resolver, config.accessKeySecretRef().orElseThrow());
            secret = resolve(resolver, config.secretKeySecretRef().orElseThrow());
            token = config.sessionTokenSecretRef().map(ref -> resolve(resolver, ref)).orElse(null);
            return StaticCredentialsProvider.create(token == null
                    ? AwsBasicCredentials.create(new String(access), new String(secret))
                    : AwsSessionCredentials.create(new String(access), new String(secret), new String(token)));
        } finally {
            if (access != null) {
                Arrays.fill(access, '\0');
            }
            if (secret != null) {
                Arrays.fill(secret, '\0');
            }
            if (token != null) {
                Arrays.fill(token, '\0');
            }
        }
    }

    private static char[] resolve(ObjectStoreSecretResolver resolver, String reference) {
        Optional<char[]> value = resolver.resolve(reference);
        return value.orElseThrow(() ->
                new IllegalArgumentException("unresolved object-store secret reference"));
    }

    private void closeCredentials() {
        AutoCloseable value = credentialsResource;
        credentialsResource = null;
        if (value != null) {
            try {
                value.close();
            } catch (Exception ignored) {
                // The client has already stopped admitting signed requests.
            }
        }
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicLong ids = new AtomicLong();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
