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

package com.nereusstream.kafka.bookkeeper.replication;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/** Strict-EOF, CRC-protected KRO1 local observation record codec. */
public final class KafkaReplicaObservationRecordCodecV1 {
    public static final int FORMAT_MAX_RECORD_BYTES = 1_152;
    private static final int MAGIC = 0x4b_52_4f_31;
    private static final int FRAME_OVERHEAD = 12;

    private KafkaReplicaObservationRecordCodecV1() {}

    public static CanonicalBytes encode(KafkaReplicaObservationRecordV1 record) {
        CanonicalBytes descriptor = KafkaReplicaCommitDescriptorCodecV1.encode(record.descriptor());
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bodyBytes)) {
                out.writeLong(record.ordinal());
                out.write(record.predecessorRecordDigest().bytes().toByteArray());
                out.writeLong(record.observedAtNanos());
                out.writeInt(descriptor.length());
                out.write(descriptor.toByteArray());
            }
            byte[] body = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(frameBytes)) {
                out.writeInt(MAGIC);
                out.writeInt(body.length);
                out.write(body);
                out.flush();
                CRC32C crc = new CRC32C();
                byte[] withoutCrc = frameBytes.toByteArray();
                crc.update(withoutCrc, 0, withoutCrc.length);
                out.writeInt((int) crc.getValue());
            }
            if (frameBytes.size() > FORMAT_MAX_RECORD_BYTES) {
                throw new IllegalArgumentException("KRO1 record exceeds its persisted cap");
            }
            return CanonicalBytes.copyOf(frameBytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory KRO1 encoding failed", failure);
        }
    }

    public static KafkaReplicaObservationRecordV1 decode(CanonicalBytes encoded) {
        byte[] bytes = encoded.toByteArray();
        if (bytes.length < FRAME_OVERHEAD || bytes.length > FORMAT_MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("KRO1 record length is outside its persisted cap");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        int storedCrc = ByteBuffer.wrap(bytes, bytes.length - Integer.BYTES, Integer.BYTES)
                .getInt();
        if ((int) crc.getValue() != storedCrc) {
            throw new IllegalArgumentException("KRO1 record CRC mismatch");
        }
        try (DataInputStream frame = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (frame.readInt() != MAGIC) {
                throw new IllegalArgumentException("KRO1 record magic/version mismatch");
            }
            int bodyLength = frame.readInt();
            if (bodyLength != bytes.length - FRAME_OVERHEAD) {
                throw new IllegalArgumentException("KRO1 record body length mismatch");
            }
            byte[] body = frame.readNBytes(bodyLength);
            if (body.length != bodyLength) {
                throw new IllegalArgumentException("KRO1 record is truncated");
            }
            frame.readInt();
            if (frame.read() != -1) {
                throw new IllegalArgumentException("KRO1 record has trailing bytes");
            }
            return decodeBody(body);
        } catch (EOFException failure) {
            throw new IllegalArgumentException("KRO1 record is truncated", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("KRO1 record cannot be decoded", failure);
        }
    }

    public static Sha256Digest digest(KafkaReplicaObservationRecordV1 record) {
        return Sha256Digest.hash(encode(record));
    }

    private static KafkaReplicaObservationRecordV1 decodeBody(byte[] body) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(body))) {
            long ordinal = in.readLong();
            byte[] predecessor = in.readNBytes(Sha256Digest.LENGTH);
            if (predecessor.length != Sha256Digest.LENGTH) {
                throw new EOFException("truncated KRO1 predecessor digest");
            }
            long observedAtNanos = in.readLong();
            int descriptorLength = in.readInt();
            if (descriptorLength <= 0
                    || descriptorLength > KafkaReplicaCommitDescriptorCodecV1.FORMAT_MAX_DESCRIPTOR_BYTES) {
                throw new IllegalArgumentException("KRO1 descriptor length exceeds its cap");
            }
            byte[] descriptor = in.readNBytes(descriptorLength);
            if (descriptor.length != descriptorLength) {
                throw new IllegalArgumentException("KRO1 descriptor is truncated");
            }
            if (in.read() != -1) {
                throw new IllegalArgumentException("KRO1 record body has unknown tail bytes");
            }
            return new KafkaReplicaObservationRecordV1(
                    ordinal,
                    Sha256Digest.copyOf(predecessor),
                    observedAtNanos,
                    KafkaReplicaCommitDescriptorCodecV1.decode(CanonicalBytes.copyOf(descriptor)));
        }
    }
}
