/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.StreamId;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GcReferenceSnapshotBuilderTest {
    @Test
    void overflowIsRetainedAsAnIncompleteVetoInsteadOfTruncatedPermission() {
        GcReferenceQuery query = query();
        var builder = new GcReferenceSnapshotBuilder(
                "test-domain-v1",
                1,
                query,
                new GcReferenceDomainConfig(1, 1, 1));
        builder.addAuthority(new GcAuthorityToken("authority/a", 1, sha256('a')));
        builder.addAuthority(new GcAuthorityToken("authority/b", 2, sha256('b')));

        GcReferenceSnapshot snapshot = builder.build();
        assertThat(snapshot.complete()).isFalse();
        assertThat(snapshot.veto()).isTrue();
        assertThat(snapshot.authorityCount()).isEqualTo(2);
        assertThat(snapshot.authorities()).hasSize(1);
    }

    @Test
    void sharedDomainBoundsRejectUnboundedOrEmptyConfigurations() {
        assertThatThrownBy(() -> new GcReferenceDomainConfig(0, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GcReferenceDomainConfig(1, 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GcReferenceDomainConfig(
                        1, 1, GcReferenceDomainConfig.MAX_SNAPSHOT_VALUES + 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void globalScopeContributesAuthoritiesAndExplicitIncompleteness() {
        GcReferenceQuery query = query();
        var builder = new GcReferenceSnapshotBuilder(
                "test-domain-v1",
                1,
                query,
                new GcReferenceDomainConfig(1, 4, 1));
        var scope = new GcGlobalReferenceScopeSnapshot(
                false,
                1,
                1,
                List.of(new StreamId("stream/reference-builder")),
                List.of(new GcAuthorityToken("authority/global", 3, sha256('d'))));

        scope.contributeTo(builder);

        GcReferenceSnapshot snapshot = builder.build();
        assertThat(snapshot.complete()).isFalse();
        assertThat(snapshot.veto()).isTrue();
        assertThat(snapshot.authorities())
                .containsExactly(new GcAuthorityToken("authority/global", 3, sha256('d')));
    }

    @Test
    void completeGlobalScopeMustBeCanonicalAndCountExact() {
        assertThatThrownBy(() -> new GcGlobalReferenceScopeSnapshot(
                        true,
                        2,
                        0,
                        List.of(new StreamId("stream/a")),
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GcGlobalReferenceScopeSnapshot(
                        true,
                        2,
                        0,
                        List.of(new StreamId("stream/b"), new StreamId("stream/a")),
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static GcReferenceQuery query() {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.create(
                new ObjectKey("objects/reference-builder"),
                Optional.empty(),
                PhysicalObjectKind.OBJECT_WAL,
                8,
                new Checksum(ChecksumType.CRC32C, "00000000"),
                Optional.empty(),
                Optional.empty());
        return GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object,
                List.of(new StreamId("stream/reference-builder")),
                sha256('e'));
    }

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, String.valueOf(value).repeat(64));
    }
}
