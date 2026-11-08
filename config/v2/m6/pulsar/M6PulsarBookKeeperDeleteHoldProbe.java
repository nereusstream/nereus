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

import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;
import org.apache.bookkeeper.mledger.proto.BookKeeperDeleteState;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;

public final class M6PulsarBookKeeperDeleteHoldProbe {
    public static void main(String[] args) throws Exception {
        var cell = PulsarM6BookKeeperCellConfig.read(Path.of(args[0]));
        long ledger = Long.parseLong(args[2]);
        var topic = TopicName.get(args[3]);
        var metadata = cell.metadata();
        var origin = "oxia-nereus://" + metadata.serviceAddress() + "/" + metadata.namespace();
        try (var bookKeeper = M5BookKeeperNamespaceAuthorityV2.connectExisting(args[1], cell.bookKeeper());
                var oxia = new PulsarP1OxiaMetadataStoreProvider()
                        .create(origin, MetadataStoreConfig.builder().build(), true)) {
            if ("settled".equals(args[4])) {
                if (!bookKeeper.nativeDeleteCellBudget().snapshot().toCompletableFuture()
                        .get(5, TimeUnit.SECONDS).reservations().isEmpty()) {
                    throw new AssertionError("Original native delete still consumes its Cell slot after DONE");
                }
                System.out.println("PULSAR_M6_NATIVE_DELETE_UNKNOWN_SETTLED " + ledger);
                return;
            }
            var physical = new BookKeeperLedger(bookKeeper.namespace(), ledger);
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (true) {
                var holds = bookKeeper.nativeDeleteCellBudget().snapshot().toCompletableFuture()
                        .get(5, TimeUnit.SECONDS).reservations();
                if (holds.size() == 1 && holds.getFirst().terminalUnknown()
                        && holds.getFirst().resource().equals(physical.sha256())) {
                    var actual = bookKeeper.borrowClient().getLedgerManager().readLedgerMetadata(ledger)
                            .get(5, TimeUnit.SECONDS);
                    var stored = oxia.get("/managed-ledgers/" + topic.getPersistenceNamingEncoding())
                            .get(5, TimeUnit.SECONDS).orElseThrow();
                    var source = new MetaStoreImpl(null, null).parseManagedLedgerInfo(stored.getValue())
                            .getLedgerInfosList().stream().filter(row -> row.getLedgerId() == ledger)
                            .findFirst().orElseThrow().getOffloadContext();
                    if (!actual.getValue().isClosed() || !source.isComplete() || !source.isBookkeeperDeleted()
                            || source.getBookkeeperDeleteState() != BookKeeperDeleteState.BK_DELETE_INTENT) {
                        throw new AssertionError("Unresolved deletion changed its original native intent or ledger");
                    }
                    System.out.println("PULSAR_M6_NATIVE_DELETE_UNKNOWN_HELD " + ledger + " "
                            + holds.getFirst().nativeIntent().toHex());
                    return;
                }
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("The original BookKeeper deletion did not retain its exact Cell slot");
                }
                Thread.sleep(50);
            }
        }
    }
}
