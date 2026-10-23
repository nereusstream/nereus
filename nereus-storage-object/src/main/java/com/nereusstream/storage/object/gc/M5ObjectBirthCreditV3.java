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

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/** Permanent chosen-key capacity credit. Only assignment consumes an original SDK-qualified physical version. */
public record M5ObjectBirthCreditV3(
        Sha256Digest scope,
        M5MaterializationWorkerV1 worker,
        M5PendingOutputInventoryV2.Slot slot,
        long grantRevision,
        State state,
        Optional<PhysicalResourceIdV2.ObjectVersion> resource,
        Optional<ObjectIdentity> output) {
    public static final String PREFIX = "v3/object-birth-credits/";
    public static final int MAX_BYTES = 8192;
    private static final int MAGIC = 0x4d354243; // M5BC

    public enum State {
        RESERVED,
        ASSIGNED,
        CANCELLED_NO_DISPATCH,
        WRITE_ARMED
    }

    public M5ObjectBirthCreditV3(
            Sha256Digest scope,
            M5MaterializationWorkerV1 worker,
            M5PendingOutputInventoryV2.Slot slot,
            long grantRevision,
            State state,
            Optional<PhysicalResourceIdV2.ObjectVersion> resource) {
        this(scope, worker, slot, grantRevision, state, resource, Optional.empty());
    }

    public M5ObjectBirthCreditV3 {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(worker, "worker");
        Objects.requireNonNull(slot, "slot");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(output, "output");
        if (scope.isZero()
                || grantRevision < 2
                || (state == State.ASSIGNED) != resource.isPresent()
                || resource.filter(value ->
                                value.identityKind() != PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION)
                        .isPresent()
                || resource.filter(value -> !value.objectKey().value().equals(slot.objectKey()))
                        .isPresent()
                || (state == State.WRITE_ARMED && output.isEmpty())
                || (state == State.RESERVED && output.isPresent())
                || output.filter(value -> !value.key().equals(slot.objectKey())).isPresent()) {
            throw new IllegalArgumentException("invalid Object birth capacity credit");
        }
    }

    /** This names an intent, never a PhysicalResourceId or an invented Provider version. */
    public Sha256Digest claimId() {
        return claimId(scope, worker, slot);
    }

    /** Chosen-key lookup only; no capacity reservation, grant revision or physical version is manufactured. */
    public static String key(
            Sha256Digest scope, M5MaterializationWorkerV1 worker, M5PendingOutputInventoryV2.Slot slot) {
        return PREFIX + claimId(scope, worker, slot).toHex();
    }

    private static Sha256Digest claimId(
            Sha256Digest scope, M5MaterializationWorkerV1 worker, M5PendingOutputInventoryV2.Slot slot) {
        Objects.requireNonNull(worker, "worker");
        Objects.requireNonNull(slot, "slot");
        if (Objects.requireNonNull(scope, "scope").isZero()) {
            throw new IllegalArgumentException("Object birth lookup scope is zero");
        }
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                blob(
                        out,
                        CanonicalUtf8.fromString("NEREUS_V3_OBJECT_BIRTH_CAPACITY_CLAIM")
                                .bytes());
                out.write(scope.bytes().toByteArray());
                blob(out, worker.encode());
                slot(out, slot);
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot commit Object birth claim", impossible);
        }
    }

    public String key() {
        return PREFIX + claimId().toHex();
    }

    public M5ObjectBirthCreditV3 reserved() {
        return new M5ObjectBirthCreditV3(scope, worker, slot, grantRevision, State.RESERVED, Optional.empty());
    }

    public M5ObjectBirthCreditV3 assign(PhysicalResourceIdV2.ObjectVersion actual) {
        if (state != State.RESERVED && state != State.WRITE_ARMED) {
            throw new IllegalStateException("Object birth credit is already terminal");
        }
        return new M5ObjectBirthCreditV3(
                scope, worker, slot, grantRevision, State.ASSIGNED, Optional.of(actual), output);
    }

    /** Persist before SDK dispatch; contains the original body commitment, never an invented version. */
    public M5ObjectBirthCreditV3 arm(ObjectIdentity identity) {
        if (state != State.RESERVED) {
            throw new IllegalStateException("Object birth write already has an original intent or terminal");
        }
        return new M5ObjectBirthCreditV3(
                scope, worker, slot, grantRevision, State.WRITE_ARMED, Optional.empty(), Optional.of(identity));
    }

    public M5ObjectBirthCreditV3 beforeTerminal() {
        return output.isPresent() ? reserved().arm(output.orElseThrow()) : reserved();
    }

    public boolean isSuccessorOf(M5ObjectBirthCreditV3 previous) {
        if (!reserved().equals(previous.reserved())) {
            return false;
        }
        if (previous.state == State.RESERVED) {
            return state == State.WRITE_ARMED
                    || output.isEmpty() && (state == State.ASSIGNED || state == State.CANCELLED_NO_DISPATCH);
        }
        return previous.state == State.WRITE_ARMED
                && output.equals(previous.output)
                && (state == State.ASSIGNED || state == State.CANCELLED_NO_DISPATCH);
    }

    /** Requires this original actor's no-dispatch proof. UNKNOWN or process death cannot use this transition. */
    public M5ObjectBirthCreditV3 cancelNoDispatch() {
        if (state != State.RESERVED && state != State.WRITE_ARMED) {
            throw new IllegalStateException("Object birth credit is already terminal");
        }
        return new M5ObjectBirthCreditV3(
                scope, worker, slot, grantRevision, State.CANCELLED_NO_DISPATCH, Optional.empty(), output);
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(output.isPresent() ? 4 : 3);
                out.write(scope.bytes().toByteArray());
                blob(out, worker.encode());
                slot(out, slot);
                out.writeLong(grantRevision);
                out.writeByte(state.ordinal());
                if (resource.isPresent()) {
                    blob(out, PhysicalResourceIdCodecV2.encode(resource.orElseThrow()));
                }
                if (output.isPresent()) {
                    out.writeLong(output.orElseThrow().bodyLength());
                    out.write(output.orElseThrow().bodySha256().bytes().toByteArray());
                }
            }
            var body = CanonicalBytes.copyOf(bytes.toByteArray());
            if (body.length() > MAX_BYTES) {
                throw new IllegalArgumentException("Object birth credit exceeds its byte cap");
            }
            return body;
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot encode Object birth credit", impossible);
        }
    }

    public static M5ObjectBirthCreditV3 decode(CanonicalBytes body) {
        if (body == null || body.isEmpty() || body.length() > MAX_BYTES) {
            throw new IllegalArgumentException("Object birth credit byte cap exceeded");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(body.toByteArray()))) {
            int magic = in.readInt();
            int wire = in.readInt();
            if (magic != MAGIC || wire != 3 && wire != 4) {
                throw new IllegalArgumentException("Object birth credit magic/version differs");
            }
            var scope = Sha256Digest.copyOf(in.readNBytes(32));
            var worker = M5MaterializationWorkerV1.decode(blob(in, M5MaterializationWorkerV1.BYTES));
            int role = in.readUnsignedByte();
            if (role >= M5PendingOutputInventoryV2.Role.values().length) {
                throw new IllegalArgumentException("Object birth slot role is unsupported");
            }
            var slot = new M5PendingOutputInventoryV2.Slot(
                    M5PendingOutputInventoryV2.Role.values()[role],
                    in.readInt(),
                    new String(
                            blob(in, M5PendingOutputInventoryV2.MAX_OBJECT_KEY_BYTES)
                                    .toByteArray(),
                            StandardCharsets.UTF_8));
            long revision = in.readLong();
            int state = in.readUnsignedByte();
            if (state >= State.values().length) {
                throw new IllegalArgumentException("Object birth credit state is unsupported");
            }
            Optional<PhysicalResourceIdV2.ObjectVersion> actual = Optional.empty();
            if (state == State.ASSIGNED.ordinal()) {
                var resource = PhysicalResourceIdCodecV2.decode(blob(in, MAX_BYTES));
                if (!(resource instanceof PhysicalResourceIdV2.ObjectVersion version)) {
                    throw new IllegalArgumentException("Object birth requires an actual Object version");
                }
                actual = Optional.of(version);
            }
            Optional<ObjectIdentity> output = wire == 4
                    ? Optional.of(
                            new ObjectIdentity(slot.objectKey(), in.readLong(), Sha256Digest.copyOf(in.readNBytes(32))))
                    : Optional.empty();
            var result =
                    new M5ObjectBirthCreditV3(scope, worker, slot, revision, State.values()[state], actual, output);
            if (in.available() != 0 || !result.encode().equals(body)) {
                throw new IllegalArgumentException("Object birth credit canonical bytes differ");
            }
            return result;
        } catch (IOException failure) {
            throw new IllegalArgumentException("truncated Object birth credit", failure);
        }
    }

    private static void slot(DataOutputStream out, M5PendingOutputInventoryV2.Slot slot) throws IOException {
        out.writeByte(slot.role().ordinal());
        out.writeInt(slot.ordinal());
        blob(out, CanonicalUtf8.fromString(slot.objectKey()).bytes());
    }

    private static void blob(DataOutputStream out, CanonicalBytes value) throws IOException {
        out.writeInt(value.length());
        out.write(value.toByteArray());
    }

    private static CanonicalBytes blob(DataInputStream in, int maximum) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > maximum || size > in.available()) {
            throw new IllegalArgumentException("Object birth credit field exceeds its byte cap");
        }
        return CanonicalBytes.copyOf(in.readNBytes(size));
    }
}
