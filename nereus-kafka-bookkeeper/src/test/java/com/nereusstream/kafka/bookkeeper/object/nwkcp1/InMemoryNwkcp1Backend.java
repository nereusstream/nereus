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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class InMemoryNwkcp1Backend implements Nwkcp1BackendV1 {
    private final String walRunPrefix;
    private final Sha256Digest rootSha;
    private final Map<String, CanonicalBytes> objects = new HashMap<>();
    private final Map<String, CanonicalBytes> heads = new HashMap<>();
    boolean unknownNextCreate;
    boolean unknownNextCas;
    boolean conflictNextCreate;
    int objectReads;

    InMemoryNwkcp1Backend(String walRunPrefix, Sha256Digest rootSha) {
        this.walRunPrefix = Objects.requireNonNull(walRunPrefix, "walRunPrefix");
        this.rootSha = Objects.requireNonNull(rootSha, "rootSha");
        Nwkcp1ObjectKeyV1.headKey(walRunPrefix);
    }

    @Override
    public synchronized CompletionStage<CreateResult> conditionalCreateObject(
            String key, CanonicalBytes body, Sha256Digest bodyDigest) {
        if (conflictNextCreate) {
            conflictNextCreate = false;
            return CompletableFuture.completedFuture(new CreateResult(CreateDisposition.CONFLICT, Optional.empty()));
        }
        CanonicalBytes existing = objects.get(key);
        CreateDisposition disposition;
        if (existing == null) {
            objects.put(key, body);
            disposition = CreateDisposition.APPLIED;
        } else if (existing.equals(body) && Sha256Digest.hash(existing).equals(bodyDigest)) {
            disposition = CreateDisposition.EXISTING_EXACT;
        } else {
            disposition = CreateDisposition.CONFLICT;
        }
        if (unknownNextCreate) {
            unknownNextCreate = false;
            disposition = CreateDisposition.EXISTING_EXACT;
        }
        Optional<CreatedObjectToken> token =
                disposition == CreateDisposition.APPLIED || disposition == CreateDisposition.EXISTING_EXACT
                        ? Optional.of(new CreatedToken(key, body.length(), bodyDigest))
                        : Optional.empty();
        return CompletableFuture.completedFuture(new CreateResult(disposition, token));
    }

    @Override
    public synchronized CompletionStage<Optional<CanonicalBytes>> readCreatedObject(CreatedObjectToken token) {
        if (token == null || token.getClass() != CreatedToken.class || ((CreatedToken) token).owner != this) {
            return CompletableFuture.failedFuture(
                    new KafkaObjectCheckpointException("created token was not issued by this in-memory backend"));
        }
        CreatedToken created = (CreatedToken) token;
        objectReads++;
        return CompletableFuture.completedFuture(Optional.ofNullable(objects.get(created.key)));
    }

    @Override
    public synchronized SelectedObjectToken selectObjectFromHead(
            String headKey, CanonicalBytes exactHeadValue, Sha256Digest exactHeadValueSha256) {
        if (!headKey.equals(Nwkcp1ObjectKeyV1.headKey(walRunPrefix))
                || !Sha256Digest.hash(exactHeadValue).equals(exactHeadValueSha256)) {
            throw new KafkaObjectCheckpointException("selected token differs from exact in-memory Head value");
        }
        KafkaProtocolCheckpointHeadV1 head =
                KafkaProtocolCheckpointHeadCodecV1.decode(walRunPrefix, rootSha, exactHeadValue);
        return new SelectedToken(
                head.checkpointObjectKey(),
                head.checkpointObjectLength(),
                head.checkpointObjectDigest(),
                exactHeadValueSha256);
    }

    @Override
    public synchronized CompletionStage<Optional<CanonicalBytes>> readSelectedObject(
            SelectedObjectToken token, boolean recovery) {
        if (token == null || token.getClass() != SelectedToken.class || ((SelectedToken) token).owner != this) {
            return CompletableFuture.failedFuture(
                    new KafkaObjectCheckpointException("selected token was not issued by this in-memory backend"));
        }
        SelectedToken selected = (SelectedToken) token;
        objectReads++;
        return CompletableFuture.completedFuture(Optional.ofNullable(objects.get(selected.key)));
    }

    @Override
    public synchronized CompletionStage<Optional<CanonicalBytes>> readHead(String key) {
        return CompletableFuture.completedFuture(Optional.ofNullable(heads.get(key)));
    }

    @Override
    public synchronized CompletionStage<CasDisposition> compareAndSetHead(
            String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes replacement) {
        CanonicalBytes current = heads.get(key);
        boolean matches =
                exactExpected.map(expected -> expected.equals(current)).orElse(current == null);
        CasDisposition disposition;
        if (matches) {
            heads.put(key, replacement);
            disposition = CasDisposition.APPLIED;
        } else {
            disposition = CasDisposition.NOT_APPLIED;
        }
        if (unknownNextCas) {
            unknownNextCas = false;
            disposition = CasDisposition.UNKNOWN;
        }
        return CompletableFuture.completedFuture(disposition);
    }

    synchronized void removeHead(String key) {
        heads.remove(key);
    }

    private final class CreatedToken implements CreatedObjectToken {
        private final InMemoryNwkcp1Backend owner = InMemoryNwkcp1Backend.this;
        private final String key;
        private final long length;
        private final Sha256Digest digest;

        private CreatedToken(String key, long length, Sha256Digest digest) {
            this.key = key;
            this.length = length;
            this.digest = digest;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public Sha256Digest digest() {
            return digest;
        }
    }

    private final class SelectedToken implements SelectedObjectToken {
        private final InMemoryNwkcp1Backend owner = InMemoryNwkcp1Backend.this;
        private final String key;
        private final long length;
        private final Sha256Digest digest;
        private final Sha256Digest exactHeadValueSha256;

        private SelectedToken(String key, long length, Sha256Digest digest, Sha256Digest exactHeadValueSha256) {
            this.key = key;
            this.length = length;
            this.digest = digest;
            this.exactHeadValueSha256 = exactHeadValueSha256;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public Sha256Digest digest() {
            return digest;
        }

        @Override
        public Sha256Digest exactHeadValueSha256() {
            return exactHeadValueSha256;
        }
    }
}
