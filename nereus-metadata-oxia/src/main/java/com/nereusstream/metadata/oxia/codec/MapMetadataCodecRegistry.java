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

package com.nereusstream.metadata.oxia.codec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable metadata codec registry used by fake and future real adapters. */
public final class MapMetadataCodecRegistry implements MetadataCodecRegistry {
    private final Map<String, MetadataRecordCodec<?>> byType;
    private final Map<Class<?>, MetadataRecordCodec<?>> byClass;

    public MapMetadataCodecRegistry(List<RegisteredCodec<?>> codecs) {
        Objects.requireNonNull(codecs, "codecs");
        Map<String, MetadataRecordCodec<?>> typeMap = new HashMap<>();
        Map<Class<?>, MetadataRecordCodec<?>> classMap = new HashMap<>();
        for (RegisteredCodec<?> registered : codecs) {
            MetadataRecordCodec<?> codec = registered.codec();
            if (typeMap.putIfAbsent(codec.recordType(), codec) != null) {
                throw new IllegalArgumentException("duplicate metadata record type: " + codec.recordType());
            }
            if (classMap.putIfAbsent(registered.recordClass(), codec) != null) {
                throw new IllegalArgumentException("duplicate metadata record class: " + registered.recordClass());
            }
        }
        this.byType = Map.copyOf(typeMap);
        this.byClass = Map.copyOf(classMap);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> MetadataRecordCodec<T> codecForType(String recordType) {
        MetadataRecordCodec<?> codec = byType.get(Objects.requireNonNull(recordType, "recordType"));
        if (codec == null) {
            throw new MetadataCodecException("unknown metadata record type: " + recordType);
        }
        return (MetadataRecordCodec<T>) codec;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> MetadataRecordCodec<T> codecForClass(Class<T> recordClass) {
        MetadataRecordCodec<?> codec = byClass.get(Objects.requireNonNull(recordClass, "recordClass"));
        if (codec == null) {
            throw new MetadataCodecException("unknown metadata record class: " + recordClass.getName());
        }
        return (MetadataRecordCodec<T>) codec;
    }

    public record RegisteredCodec<T>(
            Class<T> recordClass,
            MetadataRecordCodec<T> codec) {
        public RegisteredCodec {
            Objects.requireNonNull(recordClass, "recordClass");
            Objects.requireNonNull(codec, "codec");
        }
    }
}
