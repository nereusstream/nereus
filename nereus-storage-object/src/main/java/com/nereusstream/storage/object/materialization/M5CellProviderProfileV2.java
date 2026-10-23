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

package com.nereusstream.storage.object.materialization;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/** Complete immutable scope declaration. Only actual namespace/SDK/Cell admission can qualify its references. */
public record M5CellProviderProfileV2(
        PhysicalNamespaceAuthorityBindingV2 physicalBinding,
        ProtocolCellIdentity protocolCell,
        String exclusiveNamespacePrefix,
        References references,
        String adapterVersion,
        Sha256Digest providerConfigurationSha256,
        long maximumObjectBytes,
        int maximumRangeBytes,
        int maximumListPageKeys,
        int maximumVersionTokenBytes,
        Optional<GcGracePolicyV1> gcGracePolicy) {
    private static final int MAGIC = 0x4d354350; // M5CP
    public static final int MAX_PREFIX_BYTES = 512;
    public static final int MAX_REFERENCE_BYTES = 1024;
    public static final int MAX_BYTES = PhysicalNamespaceAuthorityBindingV2.MAX_BYTES + 8192;

    /** Explicit configured minima and selected duration. Absence never means a zero-grace policy. */
    public record GcGracePolicyV1(long providerMinimumMillis, long auditMinimumMillis, long durationMillis) {
        public GcGracePolicyV1 {
            if (providerMinimumMillis < 0
                    || auditMinimumMillis < 0
                    || durationMillis <= 0
                    || durationMillis > Long.MAX_VALUE / 1_000_000
                    || durationMillis < providerMinimumMillis
                    || durationMillis < auditMinimumMillis) {
                throw new IllegalArgumentException(
                        "GC duration must be finite, positive and meet every admitted minimum");
            }
        }
    }

    public M5CellProviderProfileV2(
            PhysicalNamespaceAuthorityBindingV2 physicalBinding,
            ProtocolCellIdentity protocolCell,
            String exclusiveNamespacePrefix,
            References references,
            String adapterVersion,
            Sha256Digest providerConfigurationSha256,
            long maximumObjectBytes,
            int maximumRangeBytes,
            int maximumListPageKeys,
            int maximumVersionTokenBytes) {
        this(
                physicalBinding,
                protocolCell,
                exclusiveNamespacePrefix,
                references,
                adapterVersion,
                providerConfigurationSha256,
                maximumObjectBytes,
                maximumRangeBytes,
                maximumListPageKeys,
                maximumVersionTokenBytes,
                Optional.empty());
    }

    /** Stable operator references only. Credential values and ephemeral SDK/worker/task identities are excluded. */
    public record References(
            String securityScope,
            String credentialScope,
            String allowedEncryptionScope,
            String quotaAdmissionScope,
            String operatorOwner,
            String deleteCapabilityScope) {
        public References {
            requireText(securityScope, MAX_REFERENCE_BYTES);
            requireText(credentialScope, MAX_REFERENCE_BYTES);
            requireText(allowedEncryptionScope, MAX_REFERENCE_BYTES);
            requireText(quotaAdmissionScope, MAX_REFERENCE_BYTES);
            requireText(operatorOwner, MAX_REFERENCE_BYTES);
            requireText(deleteCapabilityScope, MAX_REFERENCE_BYTES);
        }
    }

    public M5CellProviderProfileV2 {
        Objects.requireNonNull(physicalBinding, "physicalBinding");
        Objects.requireNonNull(protocolCell, "protocolCell");
        Objects.requireNonNull(references, "references");
        gcGracePolicy = Objects.requireNonNull(gcGracePolicy, "gcGracePolicy");
        requirePrefix(exclusiveNamespacePrefix);
        requireText(adapterVersion, 128);
        if (physicalBinding.physicalNamespace().providerKind() != ProviderKind.OBJECT_PROVIDER
                || Objects.requireNonNull(providerConfigurationSha256).isZero()
                || maximumObjectBytes <= 0
                || maximumObjectBytes > M5MaterializationRecordsV1.MAX_OBJECT_BYTES
                || maximumRangeBytes <= 0
                || maximumListPageKeys <= 0
                || maximumListPageKeys > 1000
                || maximumVersionTokenBytes <= 0
                || maximumVersionTokenBytes > 1024) {
            throw new IllegalArgumentException("Cell Provider profile kind/configuration/caps differ");
        }
        ProtocolCellIdentityCodecV1.encode(protocolCell);
    }

    public Sha256Digest protocolCellSha256() {
        return Sha256Digest.hash(ProtocolCellIdentityCodecV1.encode(protocolCell));
    }

    public CellProviderScopeId scopeId() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                write(
                        out,
                        CanonicalUtf8.fromString("NEREUS_V2_M5_CELL_PROVIDER_SCOPE_V2")
                                .bytes());
                write(out, encode());
            }
            return new CellProviderScopeId(Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray())));
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot commit Cell Provider scope", impossible);
        }
    }

    /** Prefix overlap, including parent/child aliases, violates exclusive scope admission across Cells. */
    public boolean overlapsPrefix(M5CellProviderProfileV2 other) {
        return exclusiveNamespacePrefix.equals(other.exclusiveNamespacePrefix)
                || exclusiveNamespacePrefix.startsWith(other.exclusiveNamespacePrefix + "/")
                || other.exclusiveNamespacePrefix.startsWith(exclusiveNamespacePrefix + "/");
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(gcGracePolicy.isPresent() ? 3 : 2);
                write(out, physicalBinding.encode());
                write(out, ProtocolCellIdentityCodecV1.encode(protocolCell));
                writeText(out, exclusiveNamespacePrefix);
                writeText(out, references.securityScope());
                writeText(out, references.credentialScope());
                writeText(out, references.allowedEncryptionScope());
                writeText(out, references.quotaAdmissionScope());
                writeText(out, references.operatorOwner());
                writeText(out, references.deleteCapabilityScope());
                writeText(out, adapterVersion);
                out.write(providerConfigurationSha256.bytes().toByteArray());
                out.writeLong(maximumObjectBytes);
                out.writeInt(maximumRangeBytes);
                out.writeInt(maximumListPageKeys);
                out.writeInt(maximumVersionTokenBytes);
                if (gcGracePolicy.isPresent()) {
                    var policy = gcGracePolicy.orElseThrow();
                    out.writeLong(policy.providerMinimumMillis());
                    out.writeLong(policy.auditMinimumMillis());
                    out.writeLong(policy.durationMillis());
                }
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("Cell Provider profile exceeds its body cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot encode Cell Provider profile", impossible);
        }
    }

    public static M5CellProviderProfileV2 decode(CanonicalBytes bytes) {
        if (bytes == null || bytes.isEmpty() || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("Cell Provider profile body length differs");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC) {
                throw new IllegalArgumentException("Cell Provider profile type differs");
            }
            int version = in.readInt();
            if (version != 2 && version != 3) {
                throw new IllegalArgumentException("Cell Provider profile version differs");
            }
            var result = new M5CellProviderProfileV2(
                    PhysicalNamespaceAuthorityBindingV2.decode(blob(in, PhysicalNamespaceAuthorityBindingV2.MAX_BYTES)),
                    ProtocolCellIdentityCodecV1.decode(blob(in, 54).toByteArray()),
                    text(in, MAX_PREFIX_BYTES),
                    new References(
                            text(in, MAX_REFERENCE_BYTES),
                            text(in, MAX_REFERENCE_BYTES),
                            text(in, MAX_REFERENCE_BYTES),
                            text(in, MAX_REFERENCE_BYTES),
                            text(in, MAX_REFERENCE_BYTES),
                            text(in, MAX_REFERENCE_BYTES)),
                    text(in, 128),
                    Sha256Digest.copyOf(exact(in, Sha256Digest.LENGTH)),
                    in.readLong(),
                    in.readInt(),
                    in.readInt(),
                    in.readInt(),
                    version == 3
                            ? Optional.of(new GcGracePolicyV1(in.readLong(), in.readLong(), in.readLong()))
                            : Optional.empty());
            if (in.read() != -1 || !result.encode().equals(bytes)) {
                throw new IllegalArgumentException("Cell Provider profile is not exact canonical bytes");
            }
            return result;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Cell Provider profile is truncated", failure);
        }
    }

    private static CanonicalBytes blob(DataInputStream in, int maximum) throws IOException {
        int length = in.readInt();
        if (length < 1 || length > maximum || length > in.available()) {
            throw new IllegalArgumentException("Cell Provider profile field exceeds its exact cap");
        }
        return CanonicalBytes.copyOf(exact(in, length));
    }

    private static byte[] exact(DataInputStream in, int length) throws IOException {
        var result = new byte[length];
        in.readFully(result);
        return result;
    }

    private static String text(DataInputStream in, int maximum) throws IOException {
        return CanonicalUtf8.fromBytes(blob(in, maximum).toByteArray()).value();
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        write(out, CanonicalUtf8.fromString(value).bytes());
    }

    private static void write(DataOutputStream out, CanonicalBytes value) throws IOException {
        out.writeInt(value.length());
        out.write(value.toByteArray());
    }

    private static void requireText(String value, int maximum) {
        if (value == null
                || value.isEmpty()
                || value.indexOf('\0') >= 0
                || CanonicalUtf8.fromString(value).bytes().length() > maximum) {
            throw new IllegalArgumentException("Cell Provider profile text exceeds its byte cap");
        }
    }

    private static void requirePrefix(String prefix) {
        requireText(prefix, MAX_PREFIX_BYTES);
        if (prefix.startsWith("/") || prefix.endsWith("/") || prefix.contains("//")) {
            throw new IllegalArgumentException("Cell Provider prefix is not a canonical relative path");
        }
        for (var segment : prefix.split("/", -1)) {
            if (segment.equals(".") || segment.equals("..") || !segment.matches("[A-Za-z0-9._-]+")) {
                throw new IllegalArgumentException("Cell Provider prefix has a non-canonical segment");
            }
        }
    }
}
