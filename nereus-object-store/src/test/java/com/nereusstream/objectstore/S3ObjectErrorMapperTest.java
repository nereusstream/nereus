/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3ObjectErrorMapperTest {
    private static final ObjectKey KEY = new ObjectKey("raw/key/sentinel");

    @Test
    void mapsClosedHttpTableWithoutRetainingSdkThrowable() {
        assertMapped(S3ObjectErrorMapper.read(service(404), "bucket", KEY),
                ErrorCode.OBJECT_NOT_FOUND, true);
        assertMapped(S3ObjectErrorMapper.put(service(412), "bucket", KEY),
                ErrorCode.OBJECT_UPLOAD_FAILED, false);
        assertThat(S3ObjectErrorMapper.put(service(412), "bucket", KEY))
                .isInstanceOf(ObjectAlreadyExistsException.class);
        assertMapped(S3ObjectErrorMapper.put(service(409), "bucket", KEY),
                ErrorCode.OBJECT_UPLOAD_FAILED, true);
        assertMapped(S3ObjectErrorMapper.read(service(403), "bucket", KEY),
                ErrorCode.OBJECT_READ_FAILED, false);
        assertMapped(S3ObjectErrorMapper.read(service(500), "bucket", KEY),
                ErrorCode.OBJECT_READ_FAILED, true);
    }

    private static S3Exception service(int status) {
        S3Exception.Builder builder = S3Exception.builder();
        builder.statusCode(status);
        builder.message("raw/key/sentinel credential-sentinel");
        return (S3Exception) builder.build();
    }

    private static void assertMapped(
            NereusException exception,
            ErrorCode code,
            boolean retriable) {
        assertThat(exception.code()).isEqualTo(code);
        assertThat(exception.retriable()).isEqualTo(retriable);
        assertThat(exception.getCause()).isNull();
        assertThat(exception.getMessage()).doesNotContain("raw/key/sentinel", "credential-sentinel");
    }
}
