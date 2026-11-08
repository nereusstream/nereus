/*
 * Copyright 2026 NereusStream.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.bookkeeper.broker;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectExtentLocatorV1;
import com.nereusstream.storage.object.control.ObjectProviderRootConfiguration;
import com.nereusstream.storage.object.control.ObjectWalLeafKeyV1;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunHistoricalObjectReaderV1;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.io.IOException;

/** Reads a selected complete native commit set using its exact immutable locator and original Root. */
public final class KafkaObjectNativeExtentV1 {
    private KafkaObjectNativeExtentV1() {}

    /** The caller holds the source pin and must not publish consumed frames until the whole read succeeds. */
    public static void read(
            WalRunObjectSession session,
            Nwg1VerificationContextV1 verification,
            KafkaObjectExtentLocatorV1 locator,
            Nwg1ObjectReaderV1.VerifiedFrameConsumer consume)
            throws IOException {
        var identity = identity(session.rootRecord().providerConfiguration(), session.rootSha256(), locator);
        requireLocator(
                session.readRoutineNwg1AppendUnit(identity, verification, locator.firstDirectoryRow(), consume)
                        .appendUnit(),
                locator);
    }

    public static void read(
            WalRunHistoricalObjectReaderV1 reader,
            Nwg1VerificationContextV1 verification,
            KafkaObjectExtentLocatorV1 locator,
            Nwg1ObjectReaderV1.VerifiedFrameConsumer consume)
            throws IOException {
        var identity = identity(reader.rootRecord().providerConfiguration(), reader.rootSha256(), locator);
        requireLocator(reader.readAppendUnit(identity, verification, locator.firstDirectoryRow(), consume), locator);
    }

    private static ObjectIdentity identity(
            ObjectProviderRootConfiguration provider, Sha256Digest rootSha, KafkaObjectExtentLocatorV1 locator) {
        var extent = locator.extent();
        if (!rootSha.equals(extent.walRunRootSha())) {
            throw new IllegalArgumentException("Kafka Object locator belongs to another physical Root");
        }
        var leaf = new ObjectWalLeafKeyV1(
                WalLaneId.fromCode(extent.laneId()),
                extent.laneSequence(),
                Math.toIntExact(extent.directoryPrefixEnd()),
                extent.bodyLength(),
                extent.bodySha());
        return new ObjectIdentity(leaf.fullKey(provider), extent.bodyLength(), extent.bodySha());
    }

    private static void requireLocator(Nwg1ObjectReaderV1.VerifiedAppendUnit result, KafkaObjectExtentLocatorV1 locator)
            throws IOException {
        if (result.coverage0() != locator.startOffset()
                || result.coverage1() != locator.endOffsetExclusive()
                || result.firstFrameOrdinal() != locator.firstDirectoryRow()
                || result.frameCount() != locator.directoryRowCount()) {
            throw new IOException("verified Kafka Object frame range differs from its captured locator");
        }
    }
}
