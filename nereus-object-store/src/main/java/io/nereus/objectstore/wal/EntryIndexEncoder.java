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

import java.util.Objects;

public final class EntryIndexEncoder {
    private EntryIndexEncoder() {
    }

    public static byte[] encode(EntryIndex index) {
        Objects.requireNonNull(index, "index");
        WalBinary.Writer writer = new WalBinary.Writer();
        writer.int32(index.entryCount());
        writer.int32(index.recordCount());
        for (EntryIndexItem entry : index.entries()) {
            writer.int32(entry.entryOrdinal());
            writer.int64(entry.relativeBaseOffset());
            writer.int32(entry.recordCount());
            writer.int64(entry.payloadOffset());
            writer.int64(entry.payloadLength());
            writer.int64(entry.eventTimeMillis());
            writer.stringMap(entry.attributes());
        }
        return writer.toByteArray();
    }
}
