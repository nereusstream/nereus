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

package com.nereusstream.kafka.bookkeeper.adapter;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Run-facing K2 output: contiguous DATA frames whose raw Kafka bytes are ready for the K3 sequencer. */
public record KafkaNbke2AssignedAppendGroupV1(long firstDataEntryId, List<Nbke2DataV1> dataFrames) {
    public KafkaNbke2AssignedAppendGroupV1 {
        if (firstDataEntryId <= 0) {
            throw new IllegalArgumentException("the first DATA entry must follow RUN_HEADER entry zero");
        }
        dataFrames = List.copyOf(Objects.requireNonNull(dataFrames, "dataFrames"));
        if (dataFrames.isEmpty()) {
            throw new IllegalArgumentException("an assigned append group must contain at least one DATA frame");
        }
        try {
            Math.addExact(firstDataEntryId, (long) dataFrames.size() - 1L);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("assigned append-group entry range overflows", failure);
        }
    }

    public List<CanonicalBytes> encode(long ledgerId) {
        List<CanonicalBytes> encoded = new ArrayList<>(dataFrames.size());
        for (int index = 0; index < dataFrames.size(); index++) {
            long entryId = Math.addExact(firstDataEntryId, index);
            encoded.add(CanonicalBytes.copyOf(Nbke2CodecV1.encode(ledgerId, entryId, dataFrames.get(index))));
        }
        return List.copyOf(encoded);
    }
}
