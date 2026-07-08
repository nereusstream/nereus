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

package io.nereus.objectstore.wal;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class EntryIndexDecoder {
    private EntryIndexDecoder() {
    }

    public static EntryIndex decode(
            byte[] bytes,
            long slicePayloadLength,
            long minEventTimeMillis,
            long maxEventTimeMillis) {
        Objects.requireNonNull(bytes, "bytes");
        try {
            return decodeInternal(bytes, slicePayloadLength, minEventTimeMillis, maxEventTimeMillis);
        } catch (io.nereus.api.NereusException e) {
            throw e;
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw WalBinary.corrupt("invalid entry index", e);
        }
    }

    private static EntryIndex decodeInternal(
            byte[] bytes,
            long slicePayloadLength,
            long minEventTimeMillis,
            long maxEventTimeMillis) {
        WalBinary.Reader reader = new WalBinary.Reader(bytes);
        int entryCount = reader.int32();
        int recordCount = reader.int32();
        if (entryCount <= 0 || recordCount <= 0) {
            throw WalBinary.corrupt("entry index counts are invalid");
        }
        EntryIndexItem[] entries = new EntryIndexItem[entryCount];
        long previousPayloadEnd = 0;
        for (int i = 0; i < entryCount; i++) {
            int entryOrdinal = reader.int32();
            long relativeBaseOffset = reader.int64();
            int itemRecordCount = reader.int32();
            long payloadOffset = reader.int64();
            long payloadLength = reader.int64();
            long eventTimeMillis = reader.int64();
            int attributeCount = reader.int32();
            if (attributeCount < 0) {
                throw WalBinary.corrupt("negative attribute count");
            }
            Map<String, String> attributes = new LinkedHashMap<>();
            for (int j = 0; j < attributeCount; j++) {
                attributes.put(reader.string(), reader.string());
            }
            EntryIndexItem item = new EntryIndexItem(
                    entryOrdinal,
                    relativeBaseOffset,
                    itemRecordCount,
                    payloadOffset,
                    payloadLength,
                    eventTimeMillis,
                    attributes);
            validateItem(item, slicePayloadLength, minEventTimeMillis, maxEventTimeMillis, previousPayloadEnd);
            previousPayloadEnd = Math.max(previousPayloadEnd, item.payloadOffset() + item.payloadLength());
            entries[i] = item;
        }
        reader.requireFullyConsumed();
        return new EntryIndex(entryCount, recordCount, java.util.List.of(entries));
    }

    private static void validateItem(
            EntryIndexItem item,
            long slicePayloadLength,
            long minEventTimeMillis,
            long maxEventTimeMillis,
            long previousPayloadEnd) {
        if (item.payloadOffset() > slicePayloadLength || item.payloadLength() > slicePayloadLength - item.payloadOffset()) {
            throw WalBinary.corrupt("entry payload range exceeds slice payload");
        }
        if (item.payloadLength() > 0 && item.payloadOffset() < previousPayloadEnd) {
            throw WalBinary.corrupt("entry payload ranges overlap");
        }
        if (item.eventTimeMillis() < minEventTimeMillis || item.eventTimeMillis() > maxEventTimeMillis) {
            throw WalBinary.corrupt("entry event time is outside slice range");
        }
    }
}
