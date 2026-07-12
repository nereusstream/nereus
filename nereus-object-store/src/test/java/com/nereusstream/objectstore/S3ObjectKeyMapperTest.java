/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ObjectKey;
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
}
