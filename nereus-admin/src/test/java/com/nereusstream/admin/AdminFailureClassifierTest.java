/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class AdminFailureClassifierTest {

    @Test
    void classifiesWrappedTimeoutSeparatelyFromProviderFailure() {
        assertThat(AdminFailureClassifier.classify(
                        new ExecutionException(new TimeoutException("deadline")), AdminExitCode.PROVIDER_ERROR))
                .isEqualTo(AdminExitCode.TIMEOUT);
        assertThat(AdminFailureClassifier.classify(new IllegalStateException("provider"), AdminExitCode.PROVIDER_ERROR))
                .isEqualTo(AdminExitCode.PROVIDER_ERROR);
    }
}
