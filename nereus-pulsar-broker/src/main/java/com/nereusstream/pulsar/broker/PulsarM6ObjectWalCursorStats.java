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

import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;

/** Exact native cursor metadata operations; this cursor has no BookKeeper ledger or ZooKeeper state. */
final class PulsarM6ObjectWalCursorStats implements ManagedCursorMXBean {
    private final PulsarM6ObjectWalCursor cursor;
    private final LongAdder metadataSuccess = new LongAdder();
    private final LongAdder metadataFailure = new LongAdder();

    PulsarM6ObjectWalCursorStats(PulsarM6ObjectWalCursor cursor) {
        this.cursor = cursor;
    }

    void recordMetadata(boolean success) {
        (success ? metadataSuccess : metadataFailure).increment();
    }

    @Override
    public String getName() {
        return cursor.getName();
    }

    @Override
    public String getLedgerName() {
        return cursor.getManagedLedger().getName();
    }

    @Override
    public void persistToLedger(boolean success) {}

    @Override
    public void persistToZookeeper(boolean success) {}

    @Override
    public long getPersistLedgerSucceed() {
        return 0;
    }

    @Override
    public long getPersistLedgerErrors() {
        return 0;
    }

    @Override
    public long getPersistZookeeperSucceed() {
        return 0;
    }

    @Override
    public long getPersistZookeeperErrors() {
        return 0;
    }

    @Override
    public void addWriteCursorLedgerSize(long size) {}

    @Override
    public void addReadCursorLedgerSize(long size) {}

    @Override
    public long getWriteCursorLedgerSize() {
        return 0;
    }

    @Override
    public long getWriteCursorLedgerLogicalSize() {
        return 0;
    }

    @Override
    public long getReadCursorLedgerSize() {
        return 0;
    }
}
