/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ReachableCommittedAppend;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
final class MetadataGenerationZeroIndexMaterializer implements GenerationZeroIndexMaterializer {
    private final String cluster; private final OxiaMetadataStore metadata;
    MetadataGenerationZeroIndexMaterializer(String cluster, OxiaMetadataStore metadata) {
        this.cluster = Objects.requireNonNull(cluster); this.metadata = Objects.requireNonNull(metadata); }
    @Override public CompletableFuture<CommittedAppend> materialize(ReachableCommittedAppend append) {
        return metadata.materializeGenerationZero(cluster, append); }
}
