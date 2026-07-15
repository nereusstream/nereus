/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.util.List;
import org.junit.jupiter.api.Test;

class ViewReadResultTest {
    @Test
    void committedCoverageMustMatchResultCursorWhileSparseViewMayAdvanceFurther() {
        ReadResult result = new ReadResult(new StreamId("stream"), 0, 2, List.of(), false);

        new ViewReadResult(ReadView.COMMITTED, result, 2);
        new ViewReadResult(ReadView.TOPIC_COMPACTED, result, 4);

        assertThatThrownBy(() -> new ViewReadResult(ReadView.COMMITTED, result, 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ViewReadResult(ReadView.TOPIC_COMPACTED, result, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
