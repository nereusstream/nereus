/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StableAppendResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
final class MetadataStableAppendCommitter implements StableAppendCommitter {
    private final String cluster; private final OxiaMetadataStore metadata;
    MetadataStableAppendCommitter(String cluster, OxiaMetadataStore metadata) {
        this.cluster = Objects.requireNonNull(cluster); this.metadata = Objects.requireNonNull(metadata); }
    @Override public CompletableFuture<StableAppendResult> commit(CommitAppendRequest request) {
        return metadata.commitStableAppend(cluster, request); }
}
