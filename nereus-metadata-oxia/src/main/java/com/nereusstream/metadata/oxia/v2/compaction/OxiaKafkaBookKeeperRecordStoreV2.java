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

package com.nereusstream.metadata.oxia.v2.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Immutable native transport for the closed BK compaction record families under one configured Cell root.
 * Kafka's typed control store must validate task, Binding, namespace and part/descriptor relationships.
 * Calls belong on a bounded low-frequency control executor. This adapter grants no task or delete authority.
 */
public final class OxiaKafkaBookKeeperRecordStoreV2 implements CanonicalControlMetadataStore {
    private static final int MAX_KEY_BYTES = 512;
    private static final int MAX_VALUE_BYTES = 1_048_576;
    private static final String PREFIX = "v2/kafka-bk-compaction-v2/";
    private static final Pattern ROOT = Pattern.compile("/(?:[A-Za-z0-9_-]+)(?:/[A-Za-z0-9_-]+)*");
    private static final Pattern KEY = Pattern.compile(PREFIX
            + "(?:[0-9a-f]{64}/task(?:/candidate|/part/(?:0|[1-9][0-9]{0,2}))?"
            + "|views/[0-9a-f]{64}/descriptor)");
    private final OxiaConditionalClient client;
    private final String cellRoot;

    public OxiaKafkaBookKeeperRecordStoreV2(AsyncOxiaClient client, String cellRoot) {
        this(new AsyncOxiaConditionalClient(client), cellRoot);
    }

    public OxiaKafkaBookKeeperRecordStoreV2(OxiaConditionalClient client, String cellRoot) {
        this.client = Objects.requireNonNull(client, "client");
        this.cellRoot = Objects.requireNonNull(cellRoot, "cellRoot");
        if (cellRoot.length() + 1 + PREFIX.length() + 64 + "views//descriptor".length() > MAX_KEY_BYTES
                || !ROOT.matcher(cellRoot).matches()) {
            throw new IllegalArgumentException(
                    "BK compaction Cell root is noncanonical or exceeds native key capacity");
        }
    }

    public String nativeKey(String key) {
        Objects.requireNonNull(key, "key");
        if (key.length() > MAX_KEY_BYTES
                || !KEY.matcher(key).matches()
                || (key.contains("/part/") && Integer.parseInt(key.substring(key.lastIndexOf('/') + 1)) >= 256)) {
            throw new IllegalArgumentException("key is outside the closed BK compaction record route");
        }
        return cellRoot + "/" + key;
    }

    @Override
    public Optional<CanonicalBytes> get(String key) {
        String qualified = nativeKey(key);
        return client.read(qualified).toCompletableFuture().join().map(record -> {
            if (!qualified.equals(record.key())) {
                throw new IllegalStateException("native BK compaction read returned another key");
            }
            verifyValue(key, record.storedBytes());
            return record.storedBytes();
        });
    }

    @Override
    public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
        String qualified = nativeKey(key);
        verifyValue(key, value);
        try {
            client.createIfAbsent(qualified, value).toCompletableFuture().join();
        } catch (RuntimeException unknownOrConflict) {
            // Exact authoritative reread also reconciles a response lost after native create.
        }
        try {
            Optional<CanonicalBytes> observed = get(key);
            return observed.isEmpty()
                    ? ControlMutationOutcome.RESPONSE_UNKNOWN
                    : observed.orElseThrow().equals(value)
                            ? ControlMutationOutcome.APPLIED
                            : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        } catch (RuntimeException unknownRead) {
            return ControlMutationOutcome.RESPONSE_UNKNOWN;
        }
    }

    @Override
    public ControlMutationOutcome compareAndSet(String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
        nativeKey(key);
        Objects.requireNonNull(expected, "expected");
        if (expected.isPresent()) {
            throw new IllegalArgumentException("BK compaction records are immutable create-only values");
        }
        return putIfAbsent(key, value);
    }

    private static void verifyValue(String key, CanonicalBytes value) {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty() || value.length() > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("BK compaction record exceeds its native value bound");
        }
        if (key.endsWith("/task") || key.endsWith("/descriptor")) {
            String digest = key.endsWith("/task")
                    ? key.substring(PREFIX.length(), PREFIX.length() + 64)
                    : key.substring(PREFIX.length() + "views/".length(), PREFIX.length() + "views/".length() + 64);
            if (!Sha256Digest.hash(value).toHex().equals(digest)) {
                throw new IllegalArgumentException("BK compaction record content address differs");
            }
        } else if (key.endsWith("/candidate") && value.length() != Sha256Digest.LENGTH) {
            throw new IllegalArgumentException("BK compaction candidate is not an exact descriptor digest");
        }
    }
}
