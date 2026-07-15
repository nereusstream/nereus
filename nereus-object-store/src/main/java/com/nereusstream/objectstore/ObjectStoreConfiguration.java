/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.net.URI;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** Validated bootstrap configuration containing secret references, never plaintext credentials. */
public record ObjectStoreConfiguration(
        String providerClassName,
        URI endpoint,
        String region,
        String bucket,
        String prefix,
        boolean pathStyleAccess,
        Duration requestTimeout,
        ObjectPutRetryPolicy putRetryPolicy,
        int maxConnections,
        Optional<String> accessKeySecretRef,
        Optional<String> secretKeySecretRef,
        Optional<String> sessionTokenSecretRef) {
    public ObjectStoreConfiguration {
        providerClassName = requireText(providerClassName, "providerClassName");
        endpoint = requireEndpoint(endpoint);
        region = requireText(region, "region");
        bucket = requireText(bucket, "bucket");
        prefix = requireCanonicalPrefix(prefix);
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (requestTimeout.isZero() || requestTimeout.isNegative() || requestTimeout.toMillis() <= 0) {
            throw new IllegalArgumentException("requestTimeout must be positive and millisecond-representable");
        }
        Objects.requireNonNull(putRetryPolicy, "putRetryPolicy");
        if (putRetryPolicy.maxBackoff().compareTo(requestTimeout) > 0) {
            throw new IllegalArgumentException("put retry maxBackoff cannot exceed requestTimeout");
        }
        if (maxConnections <= 0) {
            throw new IllegalArgumentException("maxConnections must be positive");
        }
        accessKeySecretRef = canonicalReference(accessKeySecretRef, "accessKeySecretRef");
        secretKeySecretRef = canonicalReference(secretKeySecretRef, "secretKeySecretRef");
        sessionTokenSecretRef = canonicalReference(sessionTokenSecretRef, "sessionTokenSecretRef");
        if (accessKeySecretRef.isPresent() != secretKeySecretRef.isPresent()) {
            throw new IllegalArgumentException("access-key and secret-key references must be configured together");
        }
        if (sessionTokenSecretRef.isPresent() && accessKeySecretRef.isEmpty()) {
            throw new IllegalArgumentException("session-token reference requires access and secret key references");
        }
    }

    /** Source-compatible constructor for pre-F4 callers; guarded PUTs use the bounded default policy. */
    public ObjectStoreConfiguration(
            String providerClassName,
            URI endpoint,
            String region,
            String bucket,
            String prefix,
            boolean pathStyleAccess,
            Duration requestTimeout,
            int maxConnections,
            Optional<String> accessKeySecretRef,
            Optional<String> secretKeySecretRef,
            Optional<String> sessionTokenSecretRef) {
        this(
                providerClassName,
                endpoint,
                region,
                bucket,
                prefix,
                pathStyleAccess,
                requestTimeout,
                defaultRetryPolicy(requestTimeout),
                maxConnections,
                accessKeySecretRef,
                secretKeySecretRef,
                sessionTokenSecretRef);
    }

    private static ObjectPutRetryPolicy defaultRetryPolicy(Duration timeout) {
        Objects.requireNonNull(timeout, "requestTimeout");
        if (timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("requestTimeout must be millisecond-representable");
        }
        Duration max = timeout.compareTo(Duration.ofSeconds(1)) < 0 ? timeout : Duration.ofSeconds(1);
        Duration min = max.compareTo(Duration.ofMillis(25)) < 0 ? max : Duration.ofMillis(25);
        return new ObjectPutRetryPolicy(3, min, max);
    }

    private static URI requireEndpoint(URI endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        String scheme = endpoint.getScheme();
        String path = endpoint.getPath();
        if (!endpoint.isAbsolute()
                || (!("http".equalsIgnoreCase(scheme)) && !("https".equalsIgnoreCase(scheme)))
                || endpoint.getHost() == null
                || endpoint.getUserInfo() != null
                || endpoint.getQuery() != null
                || endpoint.getFragment() != null
                || (path != null && !path.isEmpty() && !"/".equals(path))) {
            throw new IllegalArgumentException("endpoint must be an absolute HTTP(S) origin URI");
        }
        return endpoint;
    }

    private static String requireCanonicalPrefix(String prefix) {
        String value = requireText(prefix, "prefix");
        if (value.startsWith("/") || value.endsWith("/")) {
            throw new IllegalArgumentException("prefix cannot start or end with slash");
        }
        for (String segment : value.split("/", -1)) {
            if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
                throw new IllegalArgumentException("prefix contains a non-canonical path segment");
            }
        }
        if (value.chars().anyMatch(character -> character == 0 || Character.isISOControl(character))) {
            throw new IllegalArgumentException("prefix contains a control character");
        }
        requireStrictUtf8(value, "prefix");
        return value;
    }

    private static Optional<String> canonicalReference(Optional<String> reference, String name) {
        Objects.requireNonNull(reference, name);
        return reference.map(value -> requireText(value, name));
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        requireStrictUtf8(value, name);
        return value;
    }

    private static void requireStrictUtf8(String value, String name) {
        try {
            StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(name + " must be valid UTF-8", e);
        }
    }
}
