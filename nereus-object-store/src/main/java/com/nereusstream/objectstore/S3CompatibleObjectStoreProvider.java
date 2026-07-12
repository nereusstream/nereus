/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/** Deployable AWS SDK v1 provider for AWS S3 and path-style compatible services. */
public final class S3CompatibleObjectStoreProvider implements ObjectStoreProvider {
    private final AtomicBoolean used = new AtomicBoolean();
    private volatile S3CompatibleObjectStore store;

    @Override
    public ObjectStore create(ObjectStoreConfiguration config, ObjectStoreSecretResolver resolver) {
        if (!used.compareAndSet(false, true)) throw new IllegalStateException("S3 provider creates exactly one store");
        AWSCredentialsProvider credentials = credentials(config, resolver);
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withMaxConnections(config.maxConnections())
                .withConnectionTimeout(toMillis(config.requestTimeout()))
                .withRequestTimeout(toMillis(config.requestTimeout()))
                .withSocketTimeout(toMillis(config.requestTimeout()));
        AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withClientConfiguration(clientConfig)
                .withCredentials(credentials)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        config.endpoint().toString(), config.region()))
                .withPathStyleAccessEnabled(config.pathStyleAccess())
                .build();
        store = new S3CompatibleObjectStore(client,
                Executors.newFixedThreadPool(Math.min(config.maxConnections(), 32)),
                config.bucket(), config.prefix());
        return store;
    }

    @Override public void close() { S3CompatibleObjectStore value = store; if (value != null) value.close(); }

    private static AWSCredentialsProvider credentials(ObjectStoreConfiguration config, ObjectStoreSecretResolver resolver) {
        if (config.accessKeySecretRef().isEmpty()) return DefaultAWSCredentialsProviderChain.getInstance();
        char[] access = null;
        char[] secret = null;
        char[] token = null;
        try {
            access = resolve(resolver, config.accessKeySecretRef().orElseThrow());
            secret = resolve(resolver, config.secretKeySecretRef().orElseThrow());
            token = config.sessionTokenSecretRef().map(ref -> resolve(resolver, ref)).orElse(null);
            return new AWSStaticCredentialsProvider(token == null
                    ? new BasicAWSCredentials(new String(access), new String(secret))
                    : new BasicSessionCredentials(new String(access), new String(secret), new String(token)));
        } finally {
            if (access != null) Arrays.fill(access, '\0');
            if (secret != null) Arrays.fill(secret, '\0');
            if (token != null) Arrays.fill(token, '\0');
        }
    }

    private static char[] resolve(ObjectStoreSecretResolver resolver, String reference) {
        Optional<char[]> value = resolver.resolve(reference);
        return value.orElseThrow(() -> new IllegalArgumentException("unresolved object-store secret reference"));
    }
    private static int toMillis(java.time.Duration duration) { long value = duration.toMillis(); return (int) Math.min(Integer.MAX_VALUE, Math.max(1, value)); }
}
