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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Arrays;
import java.util.Objects;

/** Stable physical identity only. Mutable eligibility and Cell access are deliberately separate. */
public sealed interface PhysicalResourceIdV2 extends Comparable<PhysicalResourceIdV2>
        permits PhysicalResourceIdV2.ObjectVersion,
                PhysicalResourceIdV2.BookKeeperLedger,
                PhysicalResourceIdV2.MultipartUpload {
    int MAX_COMPONENT_BYTES = 8192;
    int MAX_ENCODED_BYTES = 65536;

    enum ProviderKind {
        OBJECT_PROVIDER,
        BOOKKEEPER
    }

    /** Adapter-verified physical identities; neither endpoint aliases nor current credentials are namespace IDs. */
    record Namespace(ProviderKind providerKind, CanonicalUtf8 serviceIdentity, CanonicalUtf8 containerIdentity) {
        public Namespace {
            Objects.requireNonNull(providerKind, "providerKind");
            requireText(serviceIdentity, "serviceIdentity");
            requireText(containerIdentity, "containerIdentity");
        }
    }

    /** Conditional identity family must be admitted by the provider, never inferred from an ETag or local UUID. */
    enum ObjectIdentityKind {
        IMMUTABLE_VERSION,
        IMMUTABLE_CREATE
    }

    record ObjectVersion(
            Namespace namespace,
            CanonicalUtf8 objectKey,
            ObjectIdentityKind identityKind,
            CanonicalUtf8 immutableIdentity)
            implements PhysicalResourceIdV2 {
        public ObjectVersion {
            requireNamespace(namespace, ProviderKind.OBJECT_PROVIDER);
            requireText(objectKey, "objectKey");
            Objects.requireNonNull(identityKind, "identityKind");
            requireText(immutableIdentity, "immutableIdentity");
        }
    }

    /** Namespace incarnation must prevent ledger ID reuse; ensemble/fingerprint/LAC are value observations. */
    record BookKeeperLedger(Namespace namespace, long ledgerId) implements PhysicalResourceIdV2 {
        public BookKeeperLedger {
            requireNamespace(namespace, ProviderKind.BOOKKEEPER);
            if (ledgerId < 0) {
                throw new IllegalArgumentException("ledgerId must be non-negative");
            }
        }
    }

    record MultipartUpload(Namespace namespace, CanonicalUtf8 objectKey, CanonicalUtf8 uploadId)
            implements PhysicalResourceIdV2 {
        public MultipartUpload {
            requireNamespace(namespace, ProviderKind.OBJECT_PROVIDER);
            requireText(objectKey, "objectKey");
            requireText(uploadId, "uploadId");
        }
    }

    Namespace namespace();

    default CanonicalBytes canonicalBytes() {
        return PhysicalResourceIdCodecV2.encode(this);
    }

    default Sha256Digest sha256() {
        return PhysicalResourceIdCodecV2.sha256(this);
    }

    /** All users of a namespace must additionally resolve the same admitted metadata authority route. */
    default String authorityKey() {
        return "v2/physical-delete-m5-v2/" + sha256().toHex() + "/authority-v2";
    }

    @Override
    default int compareTo(PhysicalResourceIdV2 other) {
        Objects.requireNonNull(other, "other");
        return Arrays.compareUnsigned(
                canonicalBytes().toByteArray(), other.canonicalBytes().toByteArray());
    }

    private static void requireNamespace(Namespace namespace, ProviderKind kind) {
        Objects.requireNonNull(namespace, "namespace");
        if (namespace.providerKind() != kind) {
            throw new IllegalArgumentException("resource and namespace provider kinds differ");
        }
    }

    private static void requireText(CanonicalUtf8 text, String name) {
        Objects.requireNonNull(text, name);
        if (text.bytes().isEmpty() || text.bytes().length() > MAX_COMPONENT_BYTES) {
            throw new IllegalArgumentException(name + " must fit the non-empty physical identity component cap");
        }
    }
}
