/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.objectstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ObjectStoreListDeleteContractTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    @TempDir
    Path temporary;

    @Test
    void localFixturePagesOneLogicalPrefixWithoutDuplicates() {
        try (LocalFileObjectStore store = new LocalFileObjectStore(temporary.resolve("objects"))) {
            put(store, "compact/a", "a");
            put(store, "compact/b", "bb");
            put(store, "compact/c", "ccc");
            put(store, "other/d", "dddd");

            ObjectKeyPrefix prefix = new ObjectKeyPrefix("compact/");
            Optional<String> token = Optional.empty();
            List<ListedObject> listed = new ArrayList<>();
            do {
                ListObjectsResult page = store.listObjects(
                        prefix, token, new ListObjectsOptions(2, TIMEOUT)).join();
                assertThat(page.prefix()).isEqualTo(prefix);
                assertThat(page.objects()).hasSizeLessThanOrEqualTo(2);
                listed.addAll(page.objects());
                Optional<String> previous = token;
                token = page.continuationToken();
                assertThat(token).isNotEqualTo(previous);
            } while (token.isPresent());

            assertThat(listed).extracting(value -> value.key().value())
                    .containsExactly("compact/a", "compact/b", "compact/c");
            assertThat(listed).extracting(ListedObject::objectLength)
                    .containsExactly(1L, 2L, 3L);
        }
    }

    @Test
    void deleteRequiresExactImmutableIdentityAndAbsenceIsIdempotent() {
        try (LocalFileObjectStore store = new LocalFileObjectStore(temporary.resolve("delete"))) {
            PutObjectResult put = put(store, "gc/object", "immutable");
            ObjectKey key = put.key();
            DeleteObjectOptions wrong = new DeleteObjectOptions(
                    put.objectLength() + 1,
                    put.checksum(),
                    Optional.of(put.etag()),
                    TIMEOUT);

            assertThatThrownBy(() -> store.deleteObject(key, wrong).join())
                    .satisfies(error -> assertThat(unwrap(error).code())
                            .isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH));
            assertThat(store.headObject(key, new HeadObjectOptions(TIMEOUT)).join().objectLength())
                    .isEqualTo(put.objectLength());

            DeleteObjectOptions exact = new DeleteObjectOptions(
                    put.objectLength(), put.checksum(), Optional.of(put.etag()), TIMEOUT);
            assertThat(store.deleteObject(key, exact).join().status())
                    .isEqualTo(DeleteObjectResult.Status.DELETED);
            assertThat(store.deleteObject(key, exact).join().status())
                    .isEqualTo(DeleteObjectResult.Status.ALREADY_ABSENT);
        }
    }

    @Test
    void listAndDeleteValueTypesRejectUnboundedOrAmbiguousInputs() {
        assertThatThrownBy(() -> new ListObjectsOptions(0, TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ListObjectsOptions(1_001, TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ListObjectsResult(
                        new ObjectKeyPrefix("p/"), List.of(), Optional.of("next")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeleteObjectOptions(
                        -1,
                        new Checksum(ChecksumType.CRC32C, "00000000"),
                        Optional.empty(),
                        TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PutObjectResult put(LocalFileObjectStore store, String key, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return store.putObject(
                new ObjectKey(key),
                ByteBuffer.wrap(bytes),
                new PutObjectOptions(
                        "application/octet-stream",
                        Crc32cChecksums.checksum(bytes),
                        true,
                        Map.of(),
                        TIMEOUT))
                .join();
    }

    private static NereusException unwrap(Throwable supplied) {
        Throwable failure = supplied;
        while (failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        assertThat(failure).isInstanceOf(NereusException.class);
        return (NereusException) failure;
    }
}
