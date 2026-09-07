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

package com.nereusstream.storage.api.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.MultipartUpload;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectIdentityKind;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectVersion;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class PhysicalResourceIdV2Test {
    @Test
    void exactStableResourceHasIndependentGoldenEncodingAndDigest() {
        ObjectVersion resource = object("key", "v1");
        assertThat(resource.canonicalBytes().toHex())
                .isEqualTo("4d355249000201010000000773657276696365000000066275636b6574000000036b657901000000027631");
        assertThat(resource.sha256().toHex())
                .isEqualTo("6049d561ba990208f15fa9514039c93db9cff4384c0eef451c92518212369f37");
        assertThat(resource.authorityKey())
                .isEqualTo("v2/physical-delete-m5-v2/"
                        + "6049d561ba990208f15fa9514039c93db9cff4384c0eef451c92518212369f37/authority-v2");
    }

    @Test
    void allKindsRoundTripIncludingMaximumLedgerIdAndExactUnicode() {
        for (PhysicalResourceIdV2 resource : List.of(
                object("目录/a%2Fb/../c", "版本"),
                new ObjectVersion(
                        namespace(ProviderKind.OBJECT_PROVIDER),
                        text("a"),
                        ObjectIdentityKind.IMMUTABLE_CREATE,
                        text("create-id")),
                new BookKeeperLedger(namespace(ProviderKind.BOOKKEEPER), 0),
                new BookKeeperLedger(namespace(ProviderKind.BOOKKEEPER), Long.MAX_VALUE),
                new MultipartUpload(namespace(ProviderKind.OBJECT_PROVIDER), text("a"), text("upload")))) {
            assertThat(PhysicalResourceIdCodecV2.decode(resource.canonicalBytes()))
                    .isEqualTo(resource);
        }
    }

    @Test
    void realVersionNamespaceAndResourceKindChangesRemainDistinct() {
        ObjectVersion version = object("key", "v1");
        var different = List.of(
                object("key", "v2"),
                object("other-key", "v1"),
                new ObjectVersion(
                        new Namespace(ProviderKind.OBJECT_PROVIDER, text("service-2"), text("bucket")),
                        text("key"),
                        ObjectIdentityKind.IMMUTABLE_VERSION,
                        text("v1")),
                new ObjectVersion(
                        new Namespace(ProviderKind.OBJECT_PROVIDER, text("service"), text("bucket-2")),
                        text("key"),
                        ObjectIdentityKind.IMMUTABLE_VERSION,
                        text("v1")),
                new ObjectVersion(version.namespace(), text("key"), ObjectIdentityKind.IMMUTABLE_CREATE, text("v1")),
                new MultipartUpload(version.namespace(), text("key"), text("v1")));
        for (PhysicalResourceIdV2 other : different) {
            assertThat(other.sha256()).isNotEqualTo(version.sha256());
            assertThat(other.authorityKey()).isNotEqualTo(version.authorityKey());
        }
    }

    @Test
    void componentBoundariesAndNamesAreNeverNormalized() {
        assertThat(object("a/b", "c").sha256()).isNotEqualTo(object("a", "b/c").sha256());
        assertThat(object("a%2Fb", "v1").sha256())
                .isNotEqualTo(object("a/b", "v1").sha256());
        assertThat(object("a/../b", "v1").sha256())
                .isNotEqualTo(object("b", "v1").sha256());
        assertThat(object("é", "v1").sha256())
                .isNotEqualTo(object("e\u0301", "v1").sha256());
        assertThat(object("A", "v1").sha256()).isNotEqualTo(object("a", "v1").sha256());
    }

    @Test
    void rejectsMalformedLengthsTagsTruncationAndLegacyWire() {
        byte[] valid = object("key", "v1").canonicalBytes().toByteArray();
        for (int length = 0; length < valid.length; length++) {
            byte[] truncated = Arrays.copyOf(valid, length);
            assertThatThrownBy(() -> PhysicalResourceIdCodecV2.decode(CanonicalBytes.copyOf(truncated)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (int offset : List.of(0, 5, 6, 7)) {
            byte[] changed = valid.clone();
            changed[offset] = (byte) 255;
            assertThatThrownBy(() -> PhysicalResourceIdCodecV2.decode(CanonicalBytes.copyOf(changed)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (int length : List.of(-1, 0, Integer.MAX_VALUE, PhysicalResourceIdV2.MAX_COMPONENT_BYTES + 1)) {
            byte[] changed = valid.clone();
            ByteBuffer.wrap(changed).putInt(8, length);
            assertThatThrownBy(() -> PhysicalResourceIdCodecV2.decode(CanonicalBytes.copyOf(changed)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() ->
                        PhysicalResourceIdCodecV2.decode(CanonicalBytes.copyOf(Arrays.copyOf(valid, valid.length + 1))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsWrongProviderEmptyOversizeAndInvalidUtf8Components() {
        assertThatThrownBy(() -> new BookKeeperLedger(namespace(ProviderKind.OBJECT_PROVIDER), 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BookKeeperLedger(namespace(ProviderKind.BOOKKEEPER), -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> object("", "v1")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> object("a".repeat(PhysicalResourceIdV2.MAX_COMPONENT_BYTES + 1), "v1"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> object("\ud800", "v1")).isInstanceOf(IllegalArgumentException.class);
        byte[] malformed = object("key", "v1").canonicalBytes().toByteArray();
        malformed[12] = (byte) 255;
        assertThatThrownBy(() -> PhysicalResourceIdCodecV2.decode(CanonicalBytes.copyOf(malformed)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalOrderDeduplicatesMultiTargetAliasesAndKeepsDistinctVersions() {
        PhysicalResourceIdV2 first = object("key", "v1");
        PhysicalResourceIdV2 alias = PhysicalResourceIdCodecV2.decode(first.canonicalBytes());
        PhysicalResourceIdV2 second = object("key", "v2");
        assertThat(first.compareTo(alias)).isZero();
        assertThat(List.of(second, first, alias).stream().sorted().distinct().toList())
                .containsExactly(first, second);
    }

    private static ObjectVersion object(String key, String version) {
        return new ObjectVersion(
                namespace(ProviderKind.OBJECT_PROVIDER),
                text(key),
                ObjectIdentityKind.IMMUTABLE_VERSION,
                text(version));
    }

    private static Namespace namespace(ProviderKind kind) {
        return new Namespace(kind, text("service"), text("bucket"));
    }

    private static CanonicalUtf8 text(String value) {
        return CanonicalUtf8.fromString(value);
    }
}
