/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.core.read;

import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamId;
import java.util.concurrent.CompletableFuture;

/**
 * Semantic-read seam that admits only an exact externally activated generation set.
 */
public interface ConstrainedSemanticStreamReader {
    CompletableFuture<SemanticReadResult> read(
            StreamId streamId, ReadRequest request, GenerationReadConstraint constraint);
}
