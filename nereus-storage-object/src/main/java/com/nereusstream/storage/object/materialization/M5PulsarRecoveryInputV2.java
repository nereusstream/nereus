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
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/** Native ML recovery-input bytes. This declaration alone provides no actual source/worker/recovery authority. */
public record M5PulsarRecoveryInputV2(
        MetadataNamespaceIdentityV2 metadataNamespace,
        BindingIdentity binding,
        String managedLedgerName,
        String metadataKey,
        long metadataVersion,
        CanonicalBytes storedMetadataBytes) {
    private static final int MAGIC = 0x4e504d52; // NPMR
    public static final int MAX_METADATA_BYTES = 1_048_576;
    public static final int MAX_NAME_BYTES = 1024;
    public static final int MAX_KEY_BYTES = MAX_NAME_BYTES + 17;
    public static final int MAX_BYTES = MAX_METADATA_BYTES + MAX_NAME_BYTES + MAX_KEY_BYTES + 256;

    public M5PulsarRecoveryInputV2 {
        Objects.requireNonNull(metadataNamespace, "metadataNamespace");
        Objects.requireNonNull(binding, "binding");
        requireText(managedLedgerName, MAX_NAME_BYTES);
        requireText(metadataKey, MAX_KEY_BYTES);
        if (!metadataKey.equals("/managed-ledgers/" + managedLedgerName) || metadataVersion < 0) {
            throw new IllegalArgumentException("Native recovery input key/name/version differs");
        }
        storedMetadataBytes = CanonicalBytes.copyOf(
                Objects.requireNonNull(storedMetadataBytes).toByteArray());
        if (storedMetadataBytes.isEmpty() || storedMetadataBytes.length() > MAX_METADATA_BYTES) {
            throw new IllegalArgumentException("Native recovery metadata exceeds its nonempty byte cap");
        }
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(2);
                out.write(metadataNamespace.encode().toByteArray());
                out.write(binding.bindingId().digest().bytes().toByteArray());
                out.write(binding.incarnationSha256().bytes().toByteArray());
                out.write(binding.storageEpochSha256().bytes().toByteArray());
                writeBytes(out, CanonicalUtf8.fromString(managedLedgerName).bytes());
                writeBytes(out, CanonicalUtf8.fromString(metadataKey).bytes());
                out.writeLong(metadataVersion);
                writeBytes(out, storedMetadataBytes);
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("Native recovery body exceeds its byte cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot encode Native recovery input", impossible);
        }
    }

    public Sha256Digest sha256() {
        var domain = CanonicalUtf8.fromString("NEREUS_V2_M5_PULSAR_NATIVE_RECOVERY_INPUT_V2")
                .bytes();
        var encoded = encode();
        var bytes = java.nio.ByteBuffer.allocate(8 + domain.length() + encoded.length())
                .putInt(domain.length())
                .put(domain.toByteArray())
                .putInt(encoded.length())
                .put(encoded.toByteArray())
                .array();
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes));
    }

    public static M5PulsarRecoveryInputV2 decode(CanonicalBytes bytes) {
        if (bytes == null || bytes.isEmpty() || bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("Native recovery input length differs");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != 2) {
                throw new IllegalArgumentException("Native recovery input type/version differs");
            }
            var namespace = MetadataNamespaceIdentityV2.decode(
                    CanonicalBytes.copyOf(exact(in, MetadataNamespaceIdentityV2.IDENTITY_BYTES)));
            var binding = new BindingIdentity(new TopicBindingId(digest(in)), digest(in), digest(in));
            var name = CanonicalUtf8.fromBytes(readBytes(in, MAX_NAME_BYTES).toByteArray())
                    .value();
            var key = CanonicalUtf8.fromBytes(readBytes(in, MAX_KEY_BYTES).toByteArray())
                    .value();
            long version = in.readLong();
            var result = new M5PulsarRecoveryInputV2(
                    namespace, binding, name, key, version, readBytes(in, MAX_METADATA_BYTES));
            if (in.read() != -1 || !result.encode().equals(bytes)) {
                throw new IllegalArgumentException("Native recovery input is not exact canonical bytes");
            }
            return result;
        } catch (IOException truncated) {
            throw new IllegalArgumentException("Native recovery input is truncated", truncated);
        }
    }

    private static Sha256Digest digest(DataInputStream in) throws IOException {
        return Sha256Digest.copyOf(exact(in, Sha256Digest.LENGTH));
    }

    private static byte[] exact(DataInputStream in, int length) throws IOException {
        var bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    private static CanonicalBytes readBytes(DataInputStream in, int maximum) throws IOException {
        int length = in.readInt();
        if (length < 1 || length > maximum) {
            throw new IllegalArgumentException("Native recovery input field exceeds its byte cap");
        }
        return CanonicalBytes.copyOf(exact(in, length));
    }

    private static void writeBytes(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static void requireText(String value, int maximum) {
        if (value == null
                || value.isEmpty()
                || CanonicalUtf8.fromString(value).bytes().length() > maximum
                || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Native recovery input text exceeds its canonical byte cap");
        }
    }
}
