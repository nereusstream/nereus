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

package com.nereusstream.storage.object.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.provider.M5ObjectDeleteSessionV1.ReconciliationKindV1;
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

class M5ObjectDeleteSessionV1Test {
    @Test
    void exactVersionDeleteRequiresFullIdentityAndAuthoritativeAbsence() throws Exception {
        FakeVersionedTransport transport = new FakeVersionedTransport();
        ObjectIdentity target = transport.put("cell-a/objects/target", bytes("old"), "version-1");
        M5ObjectDeleteSessionV1 session = session(transport, "cell-a");

        CanonicalBytes token = session.readExactForDelete(target).immutableVersionToken();
        assertThat(session.deleteExactVersion(target, token))
                .isEqualTo(ObjectProviderTransport.ConditionalDeleteResult.DELETED_EXACT);
        assertThat(session.reconcile(target, "cell-a/objects/", token).kind())
                .isEqualTo(ReconciliationKindV1.AUTHORITATIVELY_ABSENT);
    }

    @Test
    void lostDeleteResponseConvergesOnlyThroughCompleteListAndFullGet() throws Exception {
        FakeVersionedTransport transport = new FakeVersionedTransport();
        ObjectIdentity target = transport.put("cell-a/objects/unknown", bytes("old"), "version-2");
        transport.unknownAfterDelete = true;
        M5ObjectDeleteSessionV1 session = session(transport, "cell-a");

        CanonicalBytes token = session.readExactForDelete(target).immutableVersionToken();
        assertThat(session.deleteExactVersion(target, token))
                .isEqualTo(ObjectProviderTransport.ConditionalDeleteResult.RESPONSE_UNKNOWN);
        var reconciliation = session.reconcile(target, "cell-a/objects/", token);
        assertThat(reconciliation.kind()).isEqualTo(ReconciliationKindV1.AUTHORITATIVELY_ABSENT);
        assertThat(reconciliation.listPages()).isEqualTo(1);
        assertThat(transport.getCalls).isEqualTo(2);
    }

    @Test
    void exactOldVersionMayRetryButRecreationQuarantinesWithoutDeletingIt() throws Exception {
        FakeVersionedTransport transport = new FakeVersionedTransport();
        ObjectIdentity target = transport.put("cell-a/objects/retry", bytes("old"), "version-3");
        M5ObjectDeleteSessionV1 session = session(transport, "cell-a");
        CanonicalBytes token = session.readExactForDelete(target).immutableVersionToken();

        assertThat(session.reconcile(target, "cell-a/objects/", token).kind())
                .isEqualTo(ReconciliationKindV1.EXACT_OLD_VERSION_REMAINS);

        ObjectIdentity recreated = transport.put("cell-a/objects/retry", bytes("new"), "version-4");
        assertThat(session.reconcile(target, "cell-a/objects/", token).kind())
                .isEqualTo(ReconciliationKindV1.DIFFERENT_VERSION_OR_BODY);
        assertThat(session.deleteExactVersion(recreated, token))
                .isEqualTo(ObjectProviderTransport.ConditionalDeleteResult.VERSION_PRECONDITION_FAILED);
        assertThat(transport.objects).containsKey(recreated.key());
    }

    @Test
    void unsupportedOrForeignProviderAuthorityFailsBeforeDeleteIo() {
        FakeVersionedTransport transport = new FakeVersionedTransport();
        transport.deleteSupported = false;
        assertThatThrownBy(() -> session(transport, "cell-a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("VERSION_MATCH_DELETE_V1");

        transport.deleteSupported = true;
        M5ObjectDeleteSessionV1 session = session(transport, "cell-a");
        ObjectIdentity foreign = identity("cell-b/objects/foreign", bytes("x"));
        assertThatThrownBy(() -> session.readExactForDelete(foreign))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact Cell namespace");
        assertThat(transport.deleteCalls).isZero();
    }

    private static M5ObjectDeleteSessionV1 session(FakeVersionedTransport transport, String namespace) {
        return new M5ObjectDeleteSessionV1(transport, scope(1), namespace, 1_024, 4, 64, 16_384, 1_024);
    }

    private static CellProviderScopeId scope(int value) {
        return new CellProviderScopeId(digest(value));
    }

    private static Sha256Digest digest(int value) {
        byte[] raw = new byte[Sha256Digest.LENGTH];
        raw[raw.length - 1] = (byte) value;
        return Sha256Digest.copyOf(raw);
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.US_ASCII));
    }

    private static ObjectIdentity identity(String key, CanonicalBytes body) {
        return new ObjectIdentity(key, body.length(), Sha256Digest.hash(body));
    }

    private static final class FakeVersionedTransport implements ObjectProviderTransport {
        private final Map<String, VersionedObject> objects = new LinkedHashMap<>();
        private boolean deleteSupported = true;
        private boolean unknownAfterDelete;
        private int getCalls;
        private int deleteCalls;

        private ObjectIdentity put(String key, CanonicalBytes body, String version) {
            objects.put(key, new VersionedObject(body, bytes(version)));
            return identity(key, body);
        }

        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities("fake-versioned", true, true, true, true, true, 1_024, 1_024, 16);
        }

        @Override
        public ObjectDeleteCapabilities deleteCapabilities() {
            return deleteSupported
                    ? new ObjectDeleteCapabilities("fake-versioned", "VERSION_MATCH_DELETE_V1", true, true, true, 64)
                    : ObjectDeleteCapabilities.unsupported("fake-versioned");
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) {
            return ConditionalCreateResult.DEFINITIVE_CONFLICT;
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException {
            getCalls++;
            VersionedObject value = objects.get(key);
            if (value == null) {
                throw new MissingObjectException();
            }
            return new StreamingObject(
                    value.body.length(),
                    0,
                    value.body.length(),
                    Optional.of(value.version),
                    new ByteArrayInputStream(value.body.toByteArray()));
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) {
            List<ListedObject> rows = new ArrayList<>();
            objects.forEach((key, value) -> {
                if (key.startsWith(prefix)) {
                    rows.add(new ListedObject(key, value.body.length(), Optional.of(value.version)));
                }
            });
            return new ListPage(rows, Optional.empty());
        }

        @Override
        public ConditionalDeleteResult deleteExactVersion(String key, CanonicalBytes exactVersionToken) {
            deleteCalls++;
            VersionedObject current = objects.get(key);
            if (current == null) {
                return ConditionalDeleteResult.DEFINITIVELY_NOT_FOUND;
            }
            if (!current.version.equals(exactVersionToken)) {
                return ConditionalDeleteResult.VERSION_PRECONDITION_FAILED;
            }
            objects.remove(key);
            return unknownAfterDelete
                    ? ConditionalDeleteResult.RESPONSE_UNKNOWN
                    : ConditionalDeleteResult.DELETED_EXACT;
        }

        @Override
        public FailureKind classifyFailure(IOException failure) {
            return failure instanceof MissingObjectException ? FailureKind.NOT_FOUND : FailureKind.FATAL;
        }
    }

    private record VersionedObject(CanonicalBytes body, CanonicalBytes version) {}

    private static final class MissingObjectException extends IOException {}
}
