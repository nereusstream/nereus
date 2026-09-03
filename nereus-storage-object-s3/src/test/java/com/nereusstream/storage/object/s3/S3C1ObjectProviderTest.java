/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.object.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3C1ObjectProviderTest {
    @Test
    void onlyExactNoSuchKeyIsAbsenceAndGeneric404RemainsUnknown() {
        IOException exact = S3C1ObjectProviderTransport.mapped("full GET", s3Failure(404, "NoSuchKey"));
        IOException generic = S3C1ObjectProviderTransport.mapped("full GET", s3Failure(404, "NotFound"));

        assertThat(exact).isInstanceOf(S3C1ProviderException.class);
        assertThat(((S3C1ProviderException) exact).kind()).isEqualTo(S3C1ProviderException.Kind.NOT_FOUND);
        assertThat(generic).isInstanceOf(S3C1ProviderException.class);
        assertThat(((S3C1ProviderException) generic).kind()).isEqualTo(S3C1ProviderException.Kind.OUTCOME_UNKNOWN);
    }

    @Test
    void conditionalPutTimeoutThrottleAndConcurrentConflictRemainUnknown() throws Exception {
        for (int status : new int[] {408, 409, 429, 500, 503}) {
            assertThat(S3C1ObjectProviderTransport.classifyConditionalCreateFailure(s3Failure(status, "Injected")))
                    .isEqualTo(ObjectProviderTransport.ConditionalCreateResult.RESPONSE_UNKNOWN);
        }
        assertThat(S3C1ObjectProviderTransport.classifyConditionalCreateFailure(s3Failure(412, "PreconditionFailed")))
                .isEqualTo(ObjectProviderTransport.ConditionalCreateResult.ALREADY_EXISTS);
        assertThat(S3C1ObjectProviderTransport.classifyConditionalCreateFailure(s3Failure(403, "AccessDenied")))
                .isEqualTo(ObjectProviderTransport.ConditionalCreateResult.DEFINITIVE_CONFLICT);
    }

    @Test
    void conditionalDeleteClassifiesEveryClosedProviderOutcome() throws Exception {
        assertThat(S3C1ObjectProviderTransport.classifyConditionalDeleteFailure(s3Failure(404, "NoSuchVersion")))
                .isEqualTo(ObjectProviderTransport.ConditionalDeleteResult.DEFINITIVELY_NOT_FOUND);
        assertThat(S3C1ObjectProviderTransport.classifyConditionalDeleteFailure(s3Failure(412, "PreconditionFailed")))
                .isEqualTo(ObjectProviderTransport.ConditionalDeleteResult.VERSION_PRECONDITION_FAILED);
        assertThat(S3C1ObjectProviderTransport.classifyConditionalDeleteFailure(s3Failure(429, "SlowDown")))
                .isEqualTo(ObjectProviderTransport.ConditionalDeleteResult.RETRYABLE);
        for (int status : new int[] {408, 409, 500, 503}) {
            assertThat(S3C1ObjectProviderTransport.classifyConditionalDeleteFailure(s3Failure(status, "Injected")))
                    .isEqualTo(ObjectProviderTransport.ConditionalDeleteResult.RESPONSE_UNKNOWN);
        }
        assertThat(S3C1ObjectProviderTransport.classifyConditionalDeleteFailure(s3Failure(403, "AccessDenied")))
                .isEqualTo(ObjectProviderTransport.ConditionalDeleteResult.DEFINITIVE_CONFLICT);
    }

    @Test
    void exactMultipartAbortClassifiesEveryClosedProviderOutcome() throws Exception {
        assertThat(S3C1ObjectProviderTransport.classifyMultipartAbortFailure(s3Failure(404, "NoSuchUpload")))
                .isEqualTo(ObjectProviderTransport.ExactMultipartAbortResult.DEFINITIVELY_NOT_FOUND);
        assertThat(S3C1ObjectProviderTransport.classifyMultipartAbortFailure(s3Failure(429, "SlowDown")))
                .isEqualTo(ObjectProviderTransport.ExactMultipartAbortResult.RETRYABLE);
        for (int status : new int[] {408, 409, 500, 503}) {
            assertThat(S3C1ObjectProviderTransport.classifyMultipartAbortFailure(s3Failure(status, "Injected")))
                    .isEqualTo(ObjectProviderTransport.ExactMultipartAbortResult.RESPONSE_UNKNOWN);
        }
        assertThat(S3C1ObjectProviderTransport.classifyMultipartAbortFailure(s3Failure(403, "AccessDenied")))
                .isEqualTo(ObjectProviderTransport.ExactMultipartAbortResult.DEFINITIVE_CONFLICT);
    }

    @Test
    void coreC1SessionResolvesFourTerminalOutcomesWithoutHead() throws Exception {
        FakeTransport transport = new FakeTransport();
        try (C1ObjectProviderSession provider = provider(transport)) {
            Body exact = body("root/lane-0/7/object", new byte[] {1, 2, 3});

            assertThat(provider.conditionalCreate(exact).outcome()).isEqualTo(ProviderObjectOutcome.APPLIED_EXACT);
            assertThat(provider.conditionalCreate(exact).outcome()).isEqualTo(ProviderObjectOutcome.EXISTING_EXACT);

            Body conflict = body("root/lane-0/7/object", new byte[] {9, 8, 7});
            assertThat(provider.conditionalCreate(conflict).outcome())
                    .isEqualTo(ProviderObjectOutcome.DEFINITIVE_CONFLICT);

            Body absent = body("root/lane-0/8/object", new byte[] {4});
            assertThat(provider.reconcileUnknown(absent.identity(), "root/lane-0/8/", 8, 100, 10_000, 1_024)
                            .objectResult()
                            .outcome())
                    .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
            // The common C1 session permanently binds a key to its first admitted body identity.  A same-key,
            // different-body retry is therefore rejected locally and must not spend another Provider GET.
            assertThat(transport.fullGetCalls).isEqualTo(2);
            assertThat(transport.listCalls).isEqualTo(1);
            assertThat(provider.acceptedOperations()).isZero();
            assertThat(provider.unknownObjectCount()).isZero();
        }
    }

    @Test
    void retryableListFailureRemainsUnknownUntilExactRecoveryCompletes() throws Exception {
        FakeTransport transport = new FakeTransport();
        transport.putResponseUnknown = true;
        try (C1ObjectProviderSession provider = provider(transport)) {
            Body absent = body("root/lane-0/9/object", new byte[] {4});
            assertThat(provider.conditionalCreate(absent).outcome()).isEqualTo(ProviderObjectOutcome.OUTCOME_UNKNOWN);

            transport.listFailure = true;
            assertThatThrownBy(
                            () -> provider.reconcileUnknown(absent.identity(), "root/lane-0/9/", 8, 100, 10_000, 1_024))
                    .isInstanceOf(S3C1ProviderException.class);
            transport.listFailure = false;

            assertThat(provider.reconcileUnknown(absent.identity(), "root/lane-0/9/", 8, 100, 10_000, 1_024)
                            .objectResult()
                            .outcome())
                    .isEqualTo(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
            assertThat(provider.acceptedOperations()).isZero();
            assertThat(provider.unknownObjectCount()).isZero();
        }
    }

    private static C1ObjectProviderSession provider(FakeTransport transport) {
        CellProviderScopeId scope = new CellProviderScopeId(
                Sha256Digest.hash(CanonicalBytes.copyOf("unit-provider-scope".getBytes(StandardCharsets.US_ASCII))));
        return new C1ObjectProviderSession(transport, scope, "root", 1_024, 1_024);
    }

    private static Body body(String key, byte[] bytes) {
        ObjectIdentity identity =
                new ObjectIdentity(key, bytes.length, Sha256Digest.hash(CanonicalBytes.copyOf(bytes)));
        return new Body(identity, bytes);
    }

    private static S3Exception s3Failure(int status, String code) {
        S3Exception.Builder builder = S3Exception.builder();
        builder.statusCode(status);
        builder.awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build());
        builder.message("injected");
        return (S3Exception) builder.build();
    }

    private record Body(ObjectIdentity identity, byte[] bytes) implements RepeatableObjectBody {
        private Body {
            bytes = bytes.clone();
        }

        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(bytes);
        }
    }

    private static final class FakeTransport implements ObjectProviderTransport {
        private final Map<String, byte[]> objects = new LinkedHashMap<>();
        private int fullGetCalls;
        private int listCalls;
        private boolean listFailure;
        private boolean putResponseUnknown;

        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities("fake", true, true, true, true, true, 1_024, 1_024, 2);
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException {
            if (putResponseUnknown) {
                return ConditionalCreateResult.RESPONSE_UNKNOWN;
            }
            if (objects.containsKey(identity.key())) {
                return ConditionalCreateResult.ALREADY_EXISTS;
            }
            objects.put(identity.key(), body.readAllBytes());
            return ConditionalCreateResult.CREATED;
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException {
            fullGetCalls++;
            byte[] bytes = objects.get(key);
            if (bytes == null) {
                throw new S3C1ProviderException(S3C1ProviderException.Kind.NOT_FOUND, "absent");
            }
            return new StreamingObject(
                    bytes.length, 0, bytes.length, Optional.empty(), new ByteArrayInputStream(bytes));
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken) {
            byte[] bytes = objects.get(key);
            byte[] range = java.util.Arrays.copyOfRange(bytes, (int) inclusiveStart, (int) exclusiveEnd);
            return new StreamingObject(
                    bytes.length, inclusiveStart, exclusiveEnd, Optional.empty(), new ByteArrayInputStream(range));
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys)
                throws IOException {
            listCalls++;
            if (listFailure) {
                throw new S3C1ProviderException(S3C1ProviderException.Kind.RETRYABLE, "injected");
            }
            List<ListedObject> matches = new ArrayList<>();
            objects.forEach((key, bytes) -> {
                if (key.startsWith(prefix)) {
                    matches.add(new ListedObject(key, bytes.length, Optional.empty()));
                }
            });
            return new ListPage(matches, Optional.empty());
        }

        @Override
        public FailureKind classifyFailure(IOException failure) {
            if (failure instanceof S3C1ProviderException typed) {
                return typed.kind() == S3C1ProviderException.Kind.NOT_FOUND
                        ? FailureKind.NOT_FOUND
                        : FailureKind.RETRYABLE;
            }
            return FailureKind.FATAL;
        }
    }
}
