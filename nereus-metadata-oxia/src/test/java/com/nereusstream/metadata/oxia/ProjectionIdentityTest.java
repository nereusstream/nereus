/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ProjectionIdentityTest {
    @Test
    void canonicalEncoderRoundTripsPresentAndAbsentProjectionRefs() {
        ProjectionRef reference = new ProjectionRef(
                ProjectionType.VIRTUAL_LEDGER,
                "nereus-ml-v1.主题");

        String present =
                ProjectionIdentity.encode(Optional.of(reference));
        String absent = ProjectionIdentity.encode(Optional.empty());

        assertThat(ProjectionIdentity.decode(present))
                .contains(reference);
        assertThat(ProjectionIdentity.decode(absent)).isEmpty();
        assertThat(absent)
                .isEqualTo(CommitSliceRequest.emptyProjectionIdentity());
        assertThat(present)
                .isEqualTo(
                        "13:projectionRef7:present14:VIRTUAL_LEDGER19:nereus-ml-v1.主题");
    }
}
