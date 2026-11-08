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
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedObjectReadIdentity;
import org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;

/** One original readonly SDK opening; capture never creates, retags or replays an offload. */
final class PulsarNativeOffloadedReferenceV2 {
    record Captured(List<PhysicalResourceIdV2> resources, Sha256Digest sdkFactsSha256) {
        Captured {
            resources = List.copyOf(resources);
        }
    }

    private final CompletableFuture<Captured> captured;
    private final CompletableFuture<Void> openingSettled = new CompletableFuture<>();
    private ReadHandle originalHandle;

    PulsarNativeOffloadedReferenceV2(
            NereusPulsarLedgerOffloaderV1 offloader,
            M5CellProviderProfileV2 profile,
            String ledgerName,
            LedgerInfo source) {
        this(offloader, profile, ledgerName, source, null);
    }

    PulsarNativeOffloadedReferenceV2(
            NereusPulsarLedgerOffloaderV1 offloader,
            M5CellProviderProfileV2 profile,
            String ledgerName,
            LedgerInfo source,
            org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedReadSourceIdentity expected) {
        var context = source.getOffloadContext();
        if (source.getLedgerId() < 0
                || !context.isComplete()
                || !context.hasUidMsb()
                || !context.hasUidLsb()
                || !context.hasDriverMetadata()
                || !context.getDriverMetadata().hasName()
                || !context.getDriverMetadata().getName().equals(offloader.getOffloadDriverName())) {
            throw new IllegalArgumentException("Native offload reference lacks its actual SDK identity");
        }
        var driver = new java.util.HashMap<String, String>(OffloadUtils.getOffloadDriverMetadata(source));
        driver.put("ManagedLedgerName", ledgerName);
        var attempt = new UUID(context.getUidMsb(), context.getUidLsb());
        var opening = expected == null
                ? offloader.readOffloaded(source.getLedgerId(), attempt, Map.copyOf(driver))
                : offloader.readOffloadedCaptured(source.getLedgerId(), attempt, Map.copyOf(driver), expected);
        captured = opening.thenCompose(handle -> {
            originalHandle = handle;
            final Captured value;
            try {
                var identity = offloader.describeOffloadedReadSource(handle);
                var metadata = handle.getLedgerMetadata().getCustomMetadata();
                if (expected != null && !identity.equals(expected)
                        || handle.getId() != source.getLedgerId()
                        || !handle.isClosed()
                        || !identity.attemptUuid().equals(attempt)
                        || Math.addExact(handle.getLastAddConfirmed(), 1) != source.getEntries()
                        || handle.getLength() != source.getSize()
                        || !java.util.Arrays.equals(
                                metadata.get("pulsar/managed-ledger"),
                                ledgerName.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        || !java.util.Arrays.equals(
                                metadata.get("application"), "pulsar".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                        || !java.util.Arrays.equals(
                                metadata.get("component"),
                                "managed-ledger".getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
                    throw new IllegalArgumentException("Native offload SDK frontier differs from original metadata");
                }
                var resources = List.<PhysicalResourceIdV2>of(
                        resource(profile, identity.root()), resource(profile, identity.data()));
                var bytes = new ByteArrayOutputStream();
                try (var out = new DataOutputStream(bytes)) {
                    out.writeInt(0x4e504f52); // NPOR
                    out.writeInt(1);
                    out.writeLong(identity.ledgerId());
                    out.writeLong(attempt.getMostSignificantBits());
                    out.writeLong(attempt.getLeastSignificantBits());
                    out.writeLong(handle.getLastAddConfirmed());
                    out.writeLong(handle.getLength());
                    for (var item : List.of(identity.root(), identity.data())) {
                        out.writeLong(item.length());
                        out.writeUTF(item.sha256());
                    }
                    for (var resource : resources) {
                        out.write(resource.sha256().bytes().toByteArray());
                    }
                }
                value = new Captured(resources, Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray())));
            } catch (Throwable failure) {
                return handle.closeAsync().thenCompose(unused -> {
                    originalHandle = null;
                    return CompletableFuture.failedFuture(failure);
                });
            }
            return handle.closeAsync().thenApply(unused -> {
                originalHandle = null;
                return value;
            });
        });
        captured.whenComplete((unused, failure) -> openingSettled.complete(null));
    }

    CompletableFuture<Captured> observe() {
        return captured.copy();
    }

    /** Original handle only. An UNKNOWN cleanup cannot authorize another SDK opening or lifetime refund. */
    CompletableFuture<Void> close() {
        return openingSettled.thenCompose(unused ->
                originalHandle == null ? CompletableFuture.completedFuture(null) : originalHandle.closeAsync());
    }

    static PhysicalResourceIdV2.ObjectVersion resource(
            M5CellProviderProfileV2 profile, OffloadedObjectReadIdentity object) {
        String version = object.version();
        if (!version.startsWith("version:")
                || version.length() == 8
                || version.substring(8).equals("null")
                || !object.key().startsWith(profile.exclusiveNamespacePrefix() + "/")
                || object.length() > profile.maximumObjectBytes()) {
            throw new IllegalArgumentException("Native Object reference differs from its actual immutable SDK scope");
        }
        var resource = new PhysicalResourceIdV2.ObjectVersion(
                profile.physicalBinding().physicalNamespace(),
                CanonicalUtf8.fromString(object.key()),
                PhysicalResourceIdV2.ObjectIdentityKind.IMMUTABLE_VERSION,
                CanonicalUtf8.fromString(version.substring(8)));
        PhysicalResourceIdCodecV2.encode(resource);
        return resource;
    }
}
