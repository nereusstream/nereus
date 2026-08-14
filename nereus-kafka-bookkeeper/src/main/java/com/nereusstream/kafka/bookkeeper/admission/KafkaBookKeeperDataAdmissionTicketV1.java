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

package com.nereusstream.kafka.bookkeeper.admission;

import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;

/** Proof that exact DATA bytes passed all hard limits before offset allocation. */
public final class KafkaBookKeeperDataAdmissionTicketV1 {
    private final CellProviderScopeId providerScopeId;
    private final int rawRecordBatchBytes;
    private final int encodedDataFrameBytes;
    private final int memberOrdinal;
    private final int memberCount;
    private final boolean terminalMember;

    KafkaBookKeeperDataAdmissionTicketV1(
            CellProviderScopeId providerScopeId,
            int rawRecordBatchBytes,
            int encodedDataFrameBytes,
            int memberOrdinal,
            int memberCount,
            boolean terminalMember) {
        this.providerScopeId = providerScopeId;
        this.rawRecordBatchBytes = rawRecordBatchBytes;
        this.encodedDataFrameBytes = encodedDataFrameBytes;
        this.memberOrdinal = memberOrdinal;
        this.memberCount = memberCount;
        this.terminalMember = terminalMember;
    }

    public CellProviderScopeId providerScopeId() {
        return providerScopeId;
    }

    public int rawRecordBatchBytes() {
        return rawRecordBatchBytes;
    }

    public int encodedDataFrameBytes() {
        return encodedDataFrameBytes;
    }

    public int memberOrdinal() {
        return memberOrdinal;
    }

    public int memberCount() {
        return memberCount;
    }

    public boolean terminalMember() {
        return terminalMember;
    }
}
