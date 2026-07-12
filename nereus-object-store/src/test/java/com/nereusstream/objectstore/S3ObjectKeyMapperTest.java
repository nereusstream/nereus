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
                .isEqualTo("nereus/prod/wal/2026/object");
    }

    @Test
    void rejectsAbsoluteTraversalEmptyAndControlSegments() {
        assertThatThrownBy(() -> new S3ObjectKeyMapper("/root")).isInstanceOf(IllegalArgumentException.class);
        S3ObjectKeyMapper mapper = new S3ObjectKeyMapper("root");
        assertThatThrownBy(() -> mapper.map(new ObjectKey("a/../b"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> mapper.map(new ObjectKey("a//b"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> mapper.map(new ObjectKey("a\nb"))).isInstanceOf(IllegalArgumentException.class);
    }
}
