/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.ReachableCommittedAppend;
import java.util.concurrent.CompletableFuture;
public interface GenerationZeroIndexMaterializer {
    CompletableFuture<CommittedAppend> materialize(ReachableCommittedAppend append);
}
