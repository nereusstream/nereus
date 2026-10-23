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

package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1.CapturedSource;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedObjectReadIdentity;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadBatch;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadLimits;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadOperation;

/**
 * Immutable identity and bounded read route for one actual sealed Native P4 source. Native ledger zero is valid.
 * The caller still owns complete Binding coverage, current M4 capability/selector and the read lifetime hazard.
 */
public final class PulsarNativeReadViewV1 {
    private static final int MAGIC = 0x4e505256; // NPRV
    private static final CanonicalUtf8 DOMAIN = CanonicalUtf8.fromString("NEREUS_V2_NATIVE_PULSAR_READ_VIEW_V1");

    private final CapturedSource source;
    private final CanonicalBytes canonicalBytes;
    private final Sha256Digest sha256;

    private PulsarNativeReadViewV1(CapturedSource source) {
        this.source = source;
        canonicalBytes = encode(source);
        sha256 = Sha256Digest.hash(canonicalBytes);
    }

    static PulsarNativeReadViewV1 fromCapturedSource(CapturedSource source) {
        return new PulsarNativeReadViewV1(source);
    }

    public BindingIdentity bindingIdentity() {
        return source.binding().bindingIdentity();
    }

    public long ledgerId() {
        return source.nativeSource().ledgerId();
    }

    public long lastAddConfirmed() {
        return source.nativeSource().lastAddConfirmed();
    }

    public List<PhysicalResourceIdV2> members() {
        return source.members();
    }

    public CanonicalBytes canonicalBytes() {
        return canonicalBytes;
    }

    public Sha256Digest sha256() {
        return sha256;
    }

    /**
     * Reads only this exact captured Native ledger/range. The supplied gate governs new source attempts on SDK
     * threads; stopping or cancelling an observer does not terminate accepted SDK work or its original buffers.
     */
    public CompletableFuture<CapturedReadBatch> readOwnedAsync(
            long requestedLedgerId,
            long firstEntry,
            long lastEntry,
            CapturedReadLimits limits,
            BooleanSupplier sourceUseOpen) {
        return readOwnedOperation(requestedLedgerId, firstEntry, lastEntry, limits, sourceUseOpen)
                .result();
    }

    public CapturedReadOperation readOwnedOperation(
            long requestedLedgerId,
            long firstEntry,
            long lastEntry,
            CapturedReadLimits limits,
            BooleanSupplier sourceUseOpen) {
        if (requestedLedgerId != ledgerId()) {
            return CapturedReadOperation.rejected(
                    new IllegalArgumentException("read differs from captured native ledger"));
        }
        return source.nativeSource().readOwnedOperation(firstEntry, lastEntry, limits, sourceUseOpen);
    }

    private static CanonicalBytes encode(CapturedSource source) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(source.preferredOnly() ? 2 : 1);
                writeBytes(
                        out,
                        source.preferredOnly()
                                ? CanonicalUtf8.fromString("NEREUS_V2_NATIVE_PULSAR_OBJECT_READ_VIEW_V1")
                                        .bytes()
                                : DOMAIN.bytes());
                var binding = source.binding().bindingIdentity();
                out.write(binding.bindingId().digest().bytes().toByteArray());
                out.write(binding.incarnationSha256().bytes().toByteArray());
                out.write(binding.storageEpochSha256().bytes().toByteArray());
                var p1 = source.binding().authority();
                out.write(p1.aggregate().canonicalStoredDigest().bytes().toByteArray());
                writeBytes(out, p1.aggregate().aggregateSourceVersion().value());
                writeBytes(out, p1.selector().metadataVersion().value());
                writeBytes(out, source.bookKeeperBinding().encode());
                writeBytes(out, source.physicalBinding().encode());
                var nativeSource = source.nativeSource();
                var state = nativeSource.sourceState();
                out.writeLong(nativeSource.ledgerId());
                out.writeLong(nativeSource.lastAddConfirmed());
                out.writeLong(nativeSource.length());
                out.writeLong(state.metadataVersion());
                out.writeLong(state.attemptUuid().getMostSignificantBits());
                out.writeLong(state.attemptUuid().getLeastSignificantBits());
                out.writeBoolean(state.offloadComplete());
                out.writeByte(
                        switch (state.bookKeeperDeleteState()) {
                            case NONE -> 1;
                            case INTENT -> 2;
                            case DONE -> 3;
                        });
                out.writeByte(
                        switch (nativeSource.readPriority()) {
                            case BOOKKEEPER_FIRST -> 1;
                            case TIERED_STORAGE_FIRST -> 2;
                        });
                writeBytes(out, CanonicalBytes.copyOf(nativeSource.sealedLedgerInfoBytes()));
                writeProperties(out, nativeSource.managedLedgerProperties());
                writeProperties(out, nativeSource.driverMetadata());
                if (!source.preferredOnly()) {
                    out.writeLong(source.bookKeeperMetadataVersion());
                }
                out.writeInt(source.members().size());
                for (var member : source.members()) {
                    writeBytes(out, PhysicalResourceIdCodecV2.encode(member));
                }
                var objects = source.objects();
                if (objects.ledgerId() != nativeSource.ledgerId()
                        || !objects.attemptUuid().equals(state.attemptUuid())
                        || !state.offloadComplete()) {
                    throw new IllegalStateException("native Object identity differs from captured sealed source");
                }
                writeObjectContent(out, objects.root());
                writeObjectContent(out, objects.data());
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("cannot encode native read view", failure);
        }
    }

    private static void writeObjectContent(DataOutputStream out, OffloadedObjectReadIdentity object)
            throws IOException {
        out.writeLong(object.length());
        out.write(HexFormat.of().parseHex(object.sha256()));
    }

    private static void writeProperties(DataOutputStream out, Map<String, String> properties) throws IOException {
        var keys = properties.keySet().stream()
                .map(CanonicalUtf8::fromString)
                .sorted((first, second) -> Arrays.compareUnsigned(
                        first.bytes().toByteArray(), second.bytes().toByteArray()))
                .toList();
        out.writeInt(keys.size());
        for (var key : keys) {
            writeBytes(out, key.bytes());
            writeBytes(
                    out, CanonicalUtf8.fromString(properties.get(key.value())).bytes());
        }
    }

    private static void writeBytes(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }
}
