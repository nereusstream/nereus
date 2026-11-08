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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.allocator.ProductionVirtualLedgerAllocator.Range64RuntimeArtifacts;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.function.Function;

record PulsarM6ObjectWalSettings(
        String zkServers,
        String zkLedgerRoot,
        Sha256Digest namespaceProof,
        Sha256Digest registry,
        Path selection,
        Range64RuntimeArtifacts artifacts,
        PulsarM6ObjectWalProvider.Configuration provider,
        int ioThreads,
        int maximumPendingAppends,
        long maximumPendingBytes) {
    PulsarM6ObjectWalSettings {
        if (ioThreads <= 0
                || ioThreads > 32
                || maximumPendingAppends <= 0
                || maximumPendingAppends > 65_536
                || maximumPendingBytes <= 0
                || !zkLedgerRoot.matches("/[A-Za-z0-9_-]+")
                || !zkServers.matches("[A-Za-z0-9_.:-]+(?:,[A-Za-z0-9_.:-]+)*")) {
            throw new IllegalArgumentException("Invalid Pulsar Object WAL provider or compatibility namespace");
        }
    }

    static PulsarM6ObjectWalSettings read(Function<String, String> value) {
        return new PulsarM6ObjectWalSettings(
                value.apply("objectWalBookKeeperZkServers"),
                value.apply("objectWalBookKeeperLedgerRoot"),
                digest(value, "objectWalNamespaceProof"),
                digest(value, "objectWalRegistrySha256"),
                Path.of(value.apply("objectWalRange64Receipt")),
                new Range64RuntimeArtifacts(
                        digest(value, "objectWalDomainJarSha256"),
                        digest(value, "objectWalMetadataSpiJarSha256"),
                        digest(value, "objectWalMetadataOxiaJarSha256")),
                PulsarM6ObjectWalProvider.Configuration.read(value),
                Integer.parseInt(value.apply("objectWalIoThreads")),
                Integer.parseInt(value.apply("objectWalPendingAppends")),
                Long.parseLong(value.apply("objectWalPendingBytes")));
    }

    CanonicalBytes selectionBytes() throws IOException {
        try (var input = Files.newInputStream(selection)) {
            return CanonicalBytes.copyOf(input.readNBytes(64 * 1024 + 1));
        }
    }

    private static Sha256Digest digest(Function<String, String> value, String name) {
        return Sha256Digest.copyOf(HexFormat.of().parseHex(value.apply(name)));
    }
}
