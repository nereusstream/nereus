/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.keys.DeterministicIds;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Closed, redacted mapping from SDK failures to the public Nereus error model. */
final class S3ObjectErrorMapper {
    private S3ObjectErrorMapper() {
    }

    static NereusException bootstrap(Throwable failure, String bucket) {
        return map(failure, Operation.HEAD, bucket, null);
    }

    static NereusException put(Throwable failure, String bucket, ObjectKey key) {
        return map(failure, Operation.PUT, bucket, key);
    }

    static NereusException read(Throwable failure, String bucket, ObjectKey key) {
        return map(failure, Operation.READ, bucket, key);
    }

    static NereusException head(Throwable failure, String bucket, ObjectKey key) {
        return map(failure, Operation.HEAD, bucket, key);
    }

    static NereusException timeout(Operation operation, String bucket, ObjectKey key) {
        return failure(ErrorCode.TIMEOUT, true, operation, bucket, key, "deadline expired");
    }

    static NereusException cancelled(Operation operation, String bucket, ObjectKey key) {
        return failure(ErrorCode.CANCELLED, false, operation, bucket, key, "cancelled");
    }

    static NereusException invalid(Operation operation, String bucket, ObjectKey key, String detail) {
        return failure(ErrorCode.INVALID_ARGUMENT, false, operation, bucket, key, detail);
    }

    static NereusException closed(Operation operation, String bucket, ObjectKey key) {
        return failure(ErrorCode.STORAGE_CLOSED, false, operation, bucket, key, "store closed");
    }

    private static NereusException map(Throwable supplied, Operation operation, String bucket, ObjectKey key) {
        Throwable failure = unwrap(Objects.requireNonNull(supplied, "failure"));
        if (failure instanceof NereusException nereus) {
            return nereus;
        }
        if (failure instanceof CancellationException) {
            return cancelled(operation, bucket, key);
        }
        if (failure instanceof TimeoutException
                || failure instanceof ApiCallTimeoutException
                || failure instanceof ApiCallAttemptTimeoutException) {
            return timeout(operation, bucket, key);
        }
        if (failure instanceof S3Exception service) {
            int status = service.statusCode();
            if (status == 404 && operation != Operation.PUT) {
                return failure(ErrorCode.OBJECT_NOT_FOUND, true, operation, bucket, key, "HTTP 404");
            }
            if (status == 412 && operation == Operation.PUT) {
                return alreadyExists(operation, bucket, key);
            }
            if (status == 409 && operation == Operation.PUT) {
                return failure(ErrorCode.OBJECT_UPLOAD_FAILED, true, operation, bucket, key, "HTTP 409");
            }
            boolean retriable = status == 408 || status == 429 || status >= 500;
            return failure(operation.fallback(), retriable, operation, bucket, key, "HTTP " + status);
        }
        if (failure instanceof SdkClientException) {
            return failure(operation.fallback(), true, operation, bucket, key, "SDK transport failure");
        }
        return failure(operation.fallback(), false, operation, bucket, key, "unexpected failure");
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException failure(
            ErrorCode code,
            boolean retriable,
            Operation operation,
            String bucket,
            ObjectKey key,
            String detail) {
        String keyHash = key == null
                ? "none"
                : DeterministicIds.stableHashComponent("nereus-s3-key-log-v1\0" + key.value());
        return new NereusException(code, retriable,
                message(operation, bucket, keyHash, detail));
    }

    private static ObjectAlreadyExistsException alreadyExists(
            Operation operation, String bucket, ObjectKey key) {
        String keyHash = DeterministicIds.stableHashComponent("nereus-s3-key-log-v1\0" + key.value());
        return new ObjectAlreadyExistsException(message(operation, bucket, keyHash, "HTTP 412"));
    }

    private static String message(
            Operation operation, String bucket, String keyHash, String detail) {
        return "S3 " + operation.label + " failed; bucket=" + safeBucket(bucket)
                + "; keyHash=" + keyHash + "; detail=" + detail;
    }

    private static String safeBucket(String bucket) {
        if (bucket == null || bucket.isBlank() || bucket.length() > 255) {
            return "invalid";
        }
        for (int index = 0; index < bucket.length(); index++) {
            char value = bucket.charAt(index);
            if (!(value == '.' || value == '-' || (value >= '0' && value <= '9')
                    || (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z'))) {
                return "invalid";
            }
        }
        return bucket;
    }

    enum Operation {
        PUT("put", ErrorCode.OBJECT_UPLOAD_FAILED),
        READ("read", ErrorCode.OBJECT_READ_FAILED),
        HEAD("head", ErrorCode.OBJECT_READ_FAILED);

        private final String label;
        private final ErrorCode fallback;

        Operation(String label, ErrorCode fallback) {
            this.label = label;
            this.fallback = fallback;
        }

        ErrorCode fallback() {
            return fallback;
        }
    }
}
