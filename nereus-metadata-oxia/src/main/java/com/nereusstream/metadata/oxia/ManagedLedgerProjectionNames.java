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

package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.keys.DeterministicIds;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** The sole constructor for deterministic F2 managed-ledger projection identities. */
public final class ManagedLedgerProjectionNames {
    public static final int MAX_MANAGED_LEDGER_NAME_BYTES = 16 * 1024;
    public static final long MIN_VIRTUAL_LEDGER_ID = 1L << 62;
    public static final int POSITION_MAPPING_VERSION = 1;
    public static final String PAYLOAD_MAPPING_ATTRIBUTE = "nereus.payloadMapping";
    public static final String PAYLOAD_MAPPING_V1 = "PULSAR_ENTRY_V1";
    public static final String STORAGE_CLASS = "nereus";
    public static final String POSITION_FORMULA_V1 = "ENTRY_ID_EQUALS_STREAM_OFFSET";

    private static final String NAME_HASH_DOMAIN = "pulsar-managed-ledger-name-v1\0";
    private static final String STREAM_NAME_DOMAIN = "pulsar-ml-v1\0";

    private ManagedLedgerProjectionNames() {
    }

    public static String requireManagedLedgerName(String managedLedgerName) {
        Objects.requireNonNull(managedLedgerName, "managedLedgerName");
        if (managedLedgerName.isBlank()) {
            throw new IllegalArgumentException("managedLedgerName cannot be blank");
        }
        if (managedLedgerName.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("managedLedgerName cannot contain NUL");
        }
        int encodedLength;
        try {
            encodedLength = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(managedLedgerName))
                    .remaining();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("managedLedgerName must be valid UTF-8", e);
        }
        if (encodedLength > MAX_MANAGED_LEDGER_NAME_BYTES) {
            throw new IllegalArgumentException("managedLedgerName exceeds the UTF-8 byte limit");
        }
        return managedLedgerName;
    }

    public static String managedLedgerNameHash(String managedLedgerName) {
        String exactName = requireManagedLedgerName(managedLedgerName);
        return DeterministicIds.stableHashComponent(NAME_HASH_DOMAIN + exactName);
    }

    public static StreamName streamName(String managedLedgerName, long incarnation) {
        String exactName = requireManagedLedgerName(managedLedgerName);
        if (incarnation < 1) {
            throw new IllegalArgumentException("incarnation must be positive");
        }
        return new StreamName(STREAM_NAME_DOMAIN + exactName + '\0' + Long.toString(incarnation));
    }

    public static StreamId streamId(String managedLedgerName, long incarnation) {
        return DeterministicIds.streamIdFor(streamName(managedLedgerName, incarnation));
    }
}
