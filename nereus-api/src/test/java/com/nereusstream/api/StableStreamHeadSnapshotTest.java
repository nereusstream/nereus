/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class StableStreamHeadSnapshotTest {
    private static final Checksum SHA256 =
            new Checksum(ChecksumType.SHA256, "a".repeat(64));

    @Test
    void representsAnEmptyHeadAndAnExactAuthoritySession() {
        StreamId streamId = new StreamId("stream-1");
        StableStreamHeadSnapshot empty = new StableStreamHeadSnapshot(
                streamId,
                StreamState.ACTIVE,
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                0,
                0,
                0,
                0,
                "",
                Optional.empty(),
                SHA256,
                1);
        AcquiredAppendSession acquired = new AcquiredAppendSession(
                new AppendSession(streamId, "writer", 1, "token", 1, 10_000),
                Optional.of(new AppendAuthority("type", "id", 2, "owner", 3)));
        StableStreamHeadSnapshot owned = new StableStreamHeadSnapshot(
                streamId,
                StreamState.ACTIVE,
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                0,
                2,
                10,
                1,
                "commit-1",
                Optional.of(acquired),
                SHA256,
                2);

        assertThat(empty.appendSession()).isEmpty();
        assertThat(empty.commitVersion()).isZero();
        assertThat(empty.commitAnchor().isGenesis()).isTrue();
        assertThat(owned.appendSession()).contains(acquired);
        assertThat(owned.commitAnchor()).isEqualTo(
                new StreamCommitAnchor(streamId, "commit-1", 2, 10, 1));
    }

    @Test
    void rejectsInconsistentCommitAnchorsSessionsAndDigests() {
        StreamId streamId = new StreamId("stream-1");
        assertThatThrownBy(() -> new StableStreamHeadSnapshot(
                        streamId, StreamState.ACTIVE, StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        0, 0, 0, 1, "", Optional.empty(), SHA256, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("commit anchor");
        assertThatThrownBy(() -> new StableStreamHeadSnapshot(
                        streamId, StreamState.ACTIVE, StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        0, 0, 0, 0, "", Optional.of(new AcquiredAppendSession(
                                new AppendSession(new StreamId("other"), "writer", 1, "token", 1, 10_000),
                                Optional.empty())), SHA256, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("another stream");
        assertThatThrownBy(() -> new StableStreamHeadSnapshot(
                        streamId, StreamState.ACTIVE, StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        0, 0, 0, 0, "", Optional.empty(),
                        new Checksum(ChecksumType.CRC32C, "00000000"), 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA256");
        assertThatThrownBy(() -> new StreamCommitAnchor(streamId, "commit-1", 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not canonical");
    }
}
