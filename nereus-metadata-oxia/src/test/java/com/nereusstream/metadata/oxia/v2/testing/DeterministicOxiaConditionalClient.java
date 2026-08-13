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

package com.nereusstream.metadata.oxia.v2.testing;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Deterministic same-key fake with explicit response-loss cuts. */
public final class DeterministicOxiaConditionalClient implements OxiaConditionalClient {
    public enum MutationMode {
        NORMAL,
        APPLY_THEN_RESPONSE_LOSS,
        RESPONSE_LOSS_WITHOUT_APPLY,
        SUCCESS_WITH_DIFFERENT_BYTES
    }

    private final Map<String, AuthorityRecord> records = new HashMap<>();
    private MutationMode nextMutationMode = MutationMode.NORMAL;
    private CanonicalBytes differentSuccessBytes = CanonicalBytes.empty();
    private Runnable beforeNextRead;
    private boolean nextReadFails;
    private int readCount;
    private int createCount;
    private int casCount;

    public void seed(String key, CanonicalBytes bytes, long version) {
        records.put(key, new AuthorityRecord(key, bytes, version));
    }

    public Optional<AuthorityRecord> stored(String key) {
        return Optional.ofNullable(records.get(key));
    }

    public void nextMutation(MutationMode mode) {
        nextMutationMode = mode;
    }

    public void nextSuccessStores(CanonicalBytes bytes) {
        differentSuccessBytes = bytes;
        nextMutationMode = MutationMode.SUCCESS_WITH_DIFFERENT_BYTES;
    }

    public void failNextRead() {
        nextReadFails = true;
    }

    public void beforeNextRead(Runnable action) {
        beforeNextRead = Objects.requireNonNull(action, "action");
    }

    public int readCount() {
        return readCount;
    }

    public int createCount() {
        return createCount;
    }

    public int casCount() {
        return casCount;
    }

    @Override
    public CompletionStage<Optional<AuthorityRecord>> read(String key) {
        readCount++;
        Runnable action = beforeNextRead;
        beforeNextRead = null;
        if (action != null) {
            action.run();
        }
        if (nextReadFails) {
            nextReadFails = false;
            return CompletableFuture.failedFuture(new IllegalStateException("scripted reread failure"));
        }
        return CompletableFuture.completedFuture(Optional.ofNullable(records.get(key)));
    }

    @Override
    public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes storedBytes) {
        createCount++;
        if (records.containsKey(key)) {
            return CompletableFuture.failedFuture(new KeyAlreadyExistsException(key));
        }
        return apply(key, storedBytes, 0);
    }

    @Override
    public CompletionStage<Void> compareAndSet(String key, CanonicalBytes storedBytes, long expectedVersionId) {
        casCount++;
        AuthorityRecord current = records.get(key);
        if (current == null || current.versionId() != expectedVersionId) {
            return CompletableFuture.failedFuture(new UnexpectedVersionIdException(key, expectedVersionId));
        }
        return apply(key, storedBytes, Math.addExact(current.versionId(), 1));
    }

    private CompletionStage<Void> apply(String key, CanonicalBytes requestedBytes, long version) {
        MutationMode mode = nextMutationMode;
        nextMutationMode = MutationMode.NORMAL;
        if (mode == MutationMode.RESPONSE_LOSS_WITHOUT_APPLY) {
            return responseLoss();
        }
        CanonicalBytes storedBytes =
                mode == MutationMode.SUCCESS_WITH_DIFFERENT_BYTES ? differentSuccessBytes : requestedBytes;
        records.put(key, new AuthorityRecord(key, storedBytes, version));
        if (mode == MutationMode.APPLY_THEN_RESPONSE_LOSS) {
            return responseLoss();
        }
        return CompletableFuture.completedFuture(null);
    }

    private static CompletionStage<Void> responseLoss() {
        return CompletableFuture.failedFuture(new IllegalStateException("scripted response loss"));
    }
}
