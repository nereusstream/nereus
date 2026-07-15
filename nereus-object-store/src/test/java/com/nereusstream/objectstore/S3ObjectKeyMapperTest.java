/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ObjectKey;
import java.util.List;
import org.junit.jupiter.api.Test;

class S3ObjectKeyMapperTest {
    @Test
    void mapsEveryKeyUnderExactlyOneCanonicalPrefix() {
        S3ObjectKeyMapper mapper = new S3ObjectKeyMapper("nereus/prod");
        assertThat(mapper.map(new ObjectKey("wal/2026/object")))
                .isEqualTo("nereus/prod/objects/v1/d2FsLzIwMjYvb2JqZWN0");
    }

    @Test
    void rejectsAbsoluteTraversalEmptyAndControlSegments() {
        assertThatThrownBy(() -> new S3ObjectKeyMapper("/root")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new S3ObjectKeyMapper("a/../b")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new S3ObjectKeyMapper("a//b")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new S3ObjectKeyMapper("a\nb")).isInstanceOf(IllegalArgumentException.class);
        assertThat(new S3ObjectKeyMapper("root").map(new ObjectKey("a/../b")))
                .startsWith("root/objects/v1/");
    }

    @Test
    void exactPrefixExpansionCoversOnlyLogicalMatchesForEveryByteRemainder() {
        S3ObjectKeyMapper mapper = new S3ObjectKeyMapper("root");
        for (String prefix : List.of("a", "ab", "abc", "abcd", "世界/")) {
            ObjectKeyPrefix logical = new ObjectKeyPrefix(prefix);
            List<String> physical = mapper.mapPrefixes(logical);
            int remainder = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8).length % 3;
            assertThat(physical).hasSize(remainder == 0 ? 1 : remainder == 1 ? 16 : 4);
            assertThat(physical).isSorted().doesNotHaveDuplicates();

            for (String suffix : List.of("", "x", "/object", "世界")) {
                String mapped = mapper.map(new ObjectKey(prefix + suffix));
                assertThat(physical.stream().filter(mapped::startsWith)).hasSize(1);
            }
            String nonMatch = mapper.map(new ObjectKey("z" + prefix));
            assertThat(physical).noneMatch(nonMatch::startsWith);
        }
    }

    @Test
    void unmapRequiresCanonicalConfiguredObjectKey() {
        S3ObjectKeyMapper mapper = new S3ObjectKeyMapper("root");
        ObjectKey key = new ObjectKey("wal/对象");
        assertThat(mapper.unmap(mapper.map(key))).isEqualTo(key);
        assertThatThrownBy(() -> mapper.unmap("other/objects/v1/YQ"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> mapper.unmap("root/objects/v1/YQ=="))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
