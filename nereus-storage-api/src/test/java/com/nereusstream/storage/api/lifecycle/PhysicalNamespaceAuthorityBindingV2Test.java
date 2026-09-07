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
import com.nereusstream.domain.identity.Id128;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class PhysicalNamespaceAuthorityBindingV2Test {
    @Test
    void immutableNamespaceMarkerBindsNativeVersionAndRejectsEveryByteCorruption() {
        var identity = new MetadataNamespaceIdentityV2(Id128.one(), 91);
        assertThat(identity.markerBytes().length()).isEqualTo(56);
        assertThat(identity.encode().length()).isEqualTo(96);
        assertThat(MetadataNamespaceIdentityV2.fromNativeMarker(identity.markerBytes(), 91))
                .isEqualTo(identity);
        assertThat(MetadataNamespaceIdentityV2.fromNativeMarker(identity.markerBytes(), 92))
                .isNotEqualTo(identity);
        assertThat(MetadataNamespaceIdentityV2.decode(identity.encode())).isEqualTo(identity);
        for (var bytes : new CanonicalBytes[] {identity.markerBytes(), identity.encode()}) {
            for (int i = 0; i < bytes.length(); i++) {
                var corrupt = bytes.toByteArray();
                corrupt[i] ^= 1;
                assertThatThrownBy(() -> {
                            if (bytes.length() == 56) {
                                MetadataNamespaceIdentityV2.fromNativeMarker(CanonicalBytes.copyOf(corrupt), 91);
                            } else {
                                MetadataNamespaceIdentityV2.decode(CanonicalBytes.copyOf(corrupt));
                            }
                        })
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
        assertThatThrownBy(() -> new MetadataNamespaceIdentityV2(Id128.zero(), 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MetadataNamespaceIdentityV2(Id128.one(), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void physicalRootSurvivesMetadataObservationAndProofChangesButAssignmentRetainsFullNativeIdentity() {
        var first = binding("service", "namespace", 1);
        var second = binding("service", "namespace", 2);
        assertThat(first.authorityRoot()).isEqualTo(second.authorityRoot());
        assertThat(first).isNotEqualTo(second);
        assertThat(first.encode()).isNotEqualTo(second.encode());
        assertThat(binding("service/namespace", "another", 1).authorityRoot())
                .isNotEqualTo(binding("service", "namespace/another", 1).authorityRoot());
        assertThat(PhysicalNamespaceAuthorityBindingV2.decode(first.encode())).isEqualTo(first);
    }

    @Test
    void maximumNamespaceAndMalformedCanonicalValuesRemainBounded() {
        var binding = binding("s".repeat(8192), "c".repeat(8192), 1);
        assertThat(binding.encode().length()).isEqualTo(PhysicalNamespaceAuthorityBindingV2.MAX_BYTES);
        assertThat(PhysicalNamespaceAuthorityBindingV2.decode(binding.encode())).isEqualTo(binding);
        var small = binding("service", "namespace", 1).encode();
        for (int i = 0; i < small.length(); i++) {
            var corrupt = small.toByteArray();
            corrupt[i] ^= 1;
            assertThatThrownBy(() -> PhysicalNamespaceAuthorityBindingV2.decode(CanonicalBytes.copyOf(corrupt)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> PhysicalNamespaceAuthorityBindingV2.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(small.toByteArray(), small.length() + 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PhysicalNamespaceAuthorityBindingV2.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(small.toByteArray(), small.length() - 1))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PhysicalNamespaceAuthorityBindingV2 binding(String service, String container, long version) {
        return new PhysicalNamespaceAuthorityBindingV2(
                new PhysicalResourceIdV2.Namespace(
                        PhysicalResourceIdV2.ProviderKind.BOOKKEEPER,
                        CanonicalUtf8.fromString(service),
                        CanonicalUtf8.fromString(container)),
                new MetadataNamespaceIdentityV2(Id128.one(), version));
    }
}
