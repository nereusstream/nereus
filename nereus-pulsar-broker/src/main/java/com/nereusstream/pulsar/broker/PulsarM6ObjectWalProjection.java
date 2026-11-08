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

package com.nereusstream.pulsar.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PlannedEntry;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PreparedNwg1Plan;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.FixedSlice;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.nwg1.GroupEncodingPlanV1;
import com.nereusstream.storage.object.nwg1.Nwg1CloseReasonV1;
import com.nereusstream.storage.object.nwg1.Nwg1CommitmentsV1;
import com.nereusstream.storage.object.nwg1.Nwg1DirectoryV1;
import com.nereusstream.storage.object.nwg1.Nwg1EnvelopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.nwg1.Nwg1ZstdV1;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** Maps exact broker-assigned Pulsar entry bytes and positions into the admitted NWG1 wire. */
final class PulsarM6ObjectWalProjection {
    private final WalRunRootRecord root;
    private final PulsarBindingKey binding;
    private final Nwg1DirectoryV1.BindingContext directoryBinding;
    private final Nwg1VerificationContextV1 verification;

    PulsarM6ObjectWalProjection(
            WalRunRootRecord root,
            PulsarNativeBindingV1 nativeBinding,
            PulsarBindingKey binding,
            FixedSlice geometry,
            CanonicalBytes creatorWitness) {
        var aggregate = nativeBinding.authority().aggregate();
        if (!root.protocolCellIdentity().equals(aggregate.binding().cellIdentity())) {
            throw new IllegalArgumentException("Object WAL Root belongs to a different Pulsar Cell");
        }
        PulsarM6ObjectWalOwner.requireWitness(creatorWitness, aggregate.canonicalStoredDigest());
        this.root = root;
        this.binding = binding;
        byte[] bindingId = aggregate.binding().bindingId().digest().bytes().toByteArray();
        byte[] epochId =
                aggregate.initialEpoch().storageEpochId().digest().bytes().toByteArray();
        byte[] witness = creatorWitness.toByteArray();
        byte[] nti1 = TopicIncarnationIdentityCodecV1.encode(aggregate.binding().incarnationIdentity())
                .toByteArray();
        var framePolicy = aggregate.initialEpoch().frameEncodingPolicy();
        directoryBinding = new Nwg1DirectoryV1.BindingContext(
                bindingId,
                epochId,
                Nwg1CommitmentsV1.ownerFence(2, 1, witness),
                nti1,
                2,
                1,
                2,
                1,
                framePolicy.kind(),
                framePolicy.formatVersion());
        verification = new Nwg1VerificationContextV1(
                root.protocolCellIdentity(),
                root.providerScopeId().digest().bytes().toByteArray(),
                WalRunControlCodec.rootSha256(root).bytes().toByteArray(),
                Nwg1EnvelopeV1.decode(root.wrappedRunKey().framedBytes().toByteArray()),
                (requested, kind, version) -> {
                    if (!Arrays.equals(bindingId, requested) || kind != 2 || version != 1) {
                        throw new IllegalArgumentException("Pulsar Object WAL creator witness request differs");
                    }
                    return witness.clone();
                },
                (payload, partition, owner, start, end) -> {
                    throw new IllegalStateException("Kafka payload cannot occur in a Pulsar Object WAL Root");
                },
                geometry.startInclusive(),
                Math.addExact(geometry.endInclusive(), 1));
    }

    Nwg1VerificationContextV1 verification() {
        return verification;
    }

    void requireBinding(PulsarBindingKey requested, Nwg1DirectoryV1.BindingContext actual) {
        if (!binding.equals(requested)
                || actual.ownerFenceKind() != directoryBinding.ownerFenceKind()
                || actual.ownerFenceVersion() != directoryBinding.ownerFenceVersion()
                || actual.positionDomainKind() != directoryBinding.positionDomainKind()
                || actual.positionDomainVersion() != directoryBinding.positionDomainVersion()
                || actual.framePolicyKind() != directoryBinding.framePolicyKind()
                || actual.framePolicyVersion() != directoryBinding.framePolicyVersion()
                || !Arrays.equals(directoryBinding.nti1Bytes(), actual.nti1Bytes())
                || !Arrays.equals(directoryBinding.bindingId(), actual.bindingId())
                || !Arrays.equals(directoryBinding.storageEpochId(), actual.storageEpochId())
                || !Arrays.equals(directoryBinding.ownerFenceCommitment(), actual.ownerFenceCommitment())) {
            throw new IllegalStateException("NWG1 context differs from the admitted native Pulsar Binding");
        }
    }

    PreparedNwg1Plan seal(WalLaneId lane, int policy, List<PlannedEntry> members, long maximumBodyBytes) {
        if (members.size() != 1) {
            throw new IllegalArgumentException("Native Pulsar WAL Run admits one entry at a time");
        }
        var member = members.get(0);
        if (!member.binding().equals(binding)
                || !verification.admitsPulsarLedger(member.position().virtualLedgerId())) {
            throw new IllegalArgumentException("Native Pulsar WAL Run admits one entry of its Binding at a time");
        }
        var position = member.position();
        var payload = member.payload();
        if (payload.length > root.nwg1AdmissionCaps().maxDecodedFrameBytes() || payload.length > maximumBodyBytes) {
            throw new IllegalArgumentException("Pulsar entry exceeds its admitted Object WAL frame");
        }
        var unit = new Nwg1DirectoryV1.PulsarAppendUnit(
                0,
                0,
                1,
                position.virtualLedgerId(),
                position.entryId(),
                member.appendCommitSetId().toByteArray(),
                newId().toByteArray(),
                Nwg1CommitmentsV1.sha256(payload));
        var encoding = payload.length == 0
                ? new Nwg1ZstdV1.EncodingResult(0, 0, payload)
                : Nwg1ZstdV1.encodeIfSmaller(payload);
        var frame = new GroupEncodingPlanV1.PlannedFrame(
                0,
                payload,
                encoding.preAeadBytes(),
                position.virtualLedgerId(),
                position.entryId(),
                encoding.codecKind(),
                encoding.codecVersion());
        return new PreparedNwg1Plan(
                new GroupEncodingPlanV1(
                        2,
                        root.shardId(),
                        root.shardRunEpoch(),
                        lane.code(),
                        policy,
                        payload.length,
                        0,
                        0,
                        Nwg1CloseReasonV1.EXPLICIT_FLUSH.code(),
                        Nwg1CommitmentsV1.protocolCell(verification.exactNpc1()),
                        verification.cellProviderScopeId(),
                        verification.walRunRootSha256(),
                        Nwg1CommitmentsV1.wrappedEnvelope(verification.envelope()),
                        List.of(directoryBinding),
                        List.of(unit),
                        List.of(frame)),
                List.of(binding));
    }

    static CanonicalBytes newId() {
        var id = UUID.randomUUID();
        return CanonicalBytes.copyOf(ByteBuffer.allocate(2 * Long.BYTES)
                .putLong(id.getMostSignificantBits())
                .putLong(id.getLeastSignificantBits())
                .array());
    }
}
