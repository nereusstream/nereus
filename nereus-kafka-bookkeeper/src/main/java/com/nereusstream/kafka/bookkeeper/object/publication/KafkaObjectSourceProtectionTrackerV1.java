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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectActiveTailStateV1;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Binding-local read-pin tracker and exact manifest retirement authority issuer. */
public final class KafkaObjectSourceProtectionTrackerV1 {
    public final class ReadPin implements AutoCloseable {
        private final KafkaObjectExtentLocatorV1 locator;
        private boolean closed;

        private ReadPin(KafkaObjectExtentLocatorV1 locator) {
            this.locator = locator;
        }

        @Override
        public void close() {
            synchronized (KafkaObjectSourceProtectionTrackerV1.this) {
                if (!closed) {
                    int count = requirePin(locator);
                    if (count == 1) {
                        activePins.remove(locator);
                    } else {
                        activePins.put(locator, count - 1);
                    }
                    closed = true;
                }
            }
        }
    }

    public static final class RetirementPlan {
        private final KafkaObjectSourceProtectionTrackerV1 owner;
        private final KafkaObjectActiveTailStateV1 before;
        private final KafkaObjectActiveTailStateV1 after;
        private final String manifestKey;
        private final Sha256Digest manifestSha;
        private final Sha256Digest sourceProtectionDigest;
        private final List<KafkaObjectExtentLocatorV1> retired;

        private RetirementPlan(
                KafkaObjectSourceProtectionTrackerV1 owner,
                KafkaObjectActiveTailStateV1 before,
                KafkaObjectActiveTailStateV1 after,
                String manifestKey,
                Sha256Digest manifestSha,
                Sha256Digest sourceProtectionDigest,
                List<KafkaObjectExtentLocatorV1> retired) {
            this.owner = owner;
            this.before = before;
            this.after = after;
            this.manifestKey = manifestKey;
            this.manifestSha = manifestSha;
            this.sourceProtectionDigest = sourceProtectionDigest;
            this.retired = retired;
        }

        public long replacementCoveredThrough() {
            return after.startOffset();
        }

        public boolean binds(KafkaObjectActiveTailStateV1 activeTail) {
            return before.equals(activeTail);
        }

        public KafkaObjectActiveTailStateV1 replacement() {
            return after;
        }

        public Sha256Digest sourceProtectionDigest() {
            return sourceProtectionDigest;
        }
    }

    private final KafkaObjectBindingKeyV1 binding;
    private final Sha256Digest walRunRootSha;
    private final Map<KafkaObjectExtentLocatorV1, Integer> activePins = new LinkedHashMap<>();
    private final Map<RetirementPlan, Boolean> issuedPlans = new IdentityHashMap<>();
    private RetirementPlan pendingPlan;

    public KafkaObjectSourceProtectionTrackerV1(KafkaObjectBindingKeyV1 binding, Sha256Digest walRunRootSha) {
        this.binding = Objects.requireNonNull(binding, "binding");
        this.walRunRootSha = Objects.requireNonNull(walRunRootSha, "walRunRootSha");
        if (walRunRootSha.isZero()) {
            throw new IllegalArgumentException("source-protection WalRun Root SHA is zero");
        }
    }

    public synchronized ReadPin pin(KafkaObjectExtentLocatorV1 locator) {
        requireLocator(locator);
        if (pendingPlan != null && pendingPlan.retired.contains(locator)) {
            throw new IllegalStateException("manifest-selected Kafka Object locator is retiring");
        }
        activePins.merge(locator, 1, Math::addExact);
        return new ReadPin(locator);
    }

    /** Issues a plan only when the exact manifest-covered prefix has no live read pin. */
    public synchronized RetirementPlan prepareManifestRetirement(
            KafkaObjectActiveTailStateV1 before,
            long replacementCoveredThrough,
            String manifestKey,
            Sha256Digest manifestSha) {
        Objects.requireNonNull(before, "before");
        Objects.requireNonNull(manifestKey, "manifestKey");
        Objects.requireNonNull(manifestSha, "manifestSha");
        int manifestKeyBytes = manifestKey.getBytes(StandardCharsets.UTF_8).length;
        if (!before.binding().equals(binding)
                || manifestSha.isZero()
                || manifestKey.isEmpty()
                || manifestKey.indexOf('\0') >= 0
                || manifestKeyBytes > 1024) {
            throw new IllegalArgumentException("manifest retirement context is not exact");
        }
        if (pendingPlan != null) {
            throw new IllegalStateException("a manifest retirement root CAS is already pending");
        }
        List<KafkaObjectExtentLocatorV1> retired = before.locators().stream()
                .filter(locator -> locator.endOffsetExclusive() <= replacementCoveredThrough)
                .toList();
        if (retired.isEmpty()) {
            throw new IllegalArgumentException("manifest retirement selects no complete locator");
        }
        for (KafkaObjectExtentLocatorV1 locator : retired) {
            requireLocator(locator);
            if (activePins.containsKey(locator)) {
                throw new IllegalStateException("manifest-covered Kafka Object locator still has a read pin");
            }
        }
        KafkaObjectActiveTailStateV1 after = retire(before, replacementCoveredThrough);
        Sha256Digest protectionDigest = protectionDigest(manifestKey, manifestSha, before, after);
        RetirementPlan plan =
                new RetirementPlan(this, before, after, manifestKey, manifestSha, protectionDigest, retired);
        issuedPlans.put(plan, Boolean.TRUE);
        pendingPlan = plan;
        return plan;
    }

    public synchronized KafkaObjectTailRetirementAuthorityV1 completeAfterRootCas(
            RetirementPlan plan, KafkaObjectCoherentProtocolSnapshotV1 selected) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(selected, "selected");
        if (plan.owner != this || pendingPlan != plan || !issuedPlans.containsKey(plan)) {
            throw new IllegalStateException("manifest retirement plan is foreign or already consumed");
        }
        for (KafkaObjectExtentLocatorV1 locator : plan.retired) {
            if (activePins.containsKey(locator)) {
                throw new IllegalStateException("manifest-covered Kafka Object locator regained a read pin");
            }
        }
        if (!selected.activeTail().equals(plan.after)
                || !selected.root()
                        .references()
                        .sourceProtection()
                        .contentDigest()
                        .equals(plan.sourceProtectionDigest)
                || !selected.root()
                        .references()
                        .activeTail()
                        .contentDigest()
                        .equals(Sha256Digest.hash(KafkaObjectStateCodecV1.activeTail(plan.after)))) {
            throw new IllegalStateException("root CAS did not select the exact manifest/source-protection cut");
        }
        issuedPlans.remove(plan);
        pendingPlan = null;
        return KafkaObjectTailRetirementAuthorityV1.afterRootCas(
                binding,
                walRunRootSha,
                plan.manifestKey,
                plan.manifestSha,
                selected.root().references().activeTail().contentDigest(),
                plan.retired);
    }

    private KafkaObjectActiveTailStateV1 retire(KafkaObjectActiveTailStateV1 before, long coveredThrough) {
        int retained = 0;
        while (retained < before.locators().size()
                && before.locators().get(retained).endOffsetExclusive() <= coveredThrough) {
            retained++;
        }
        if (retained < before.locators().size()
                && before.locators().get(retained).startOffset() != coveredThrough) {
            throw new IllegalArgumentException("manifest frontier cuts through a Kafka Object locator");
        }
        return new KafkaObjectActiveTailStateV1(
                binding,
                coveredThrough,
                before.endOffsetExclusive(),
                before.locators().subList(retained, before.locators().size()));
    }

    private Sha256Digest protectionDigest(
            String manifestKey,
            Sha256Digest manifestSha,
            KafkaObjectActiveTailStateV1 before,
            KafkaObjectActiveTailStateV1 after) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.writeBytes("M3-KAFKA-OBJECT-MANIFEST-PINS-DRAINED-V1".getBytes(StandardCharsets.UTF_8));
        bytes.writeBytes(walRunRootSha.bytes().toByteArray());
        bytes.writeBytes(manifestKey.getBytes(StandardCharsets.UTF_8));
        bytes.writeBytes(manifestSha.bytes().toByteArray());
        bytes.writeBytes(KafkaObjectStateCodecV1.activeTail(before).toByteArray());
        bytes.writeBytes(KafkaObjectStateCodecV1.activeTail(after).toByteArray());
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
    }

    private void requireLocator(KafkaObjectExtentLocatorV1 locator) {
        Objects.requireNonNull(locator, "locator");
        if (!locator.binding().equals(binding)
                || !locator.extent().walRunRootSha().equals(walRunRootSha)) {
            throw new IllegalArgumentException("source-protection locator differs from binding/Root authority");
        }
    }

    private int requirePin(KafkaObjectExtentLocatorV1 locator) {
        Integer count = activePins.get(locator);
        if (count == null || count <= 0) {
            throw new IllegalStateException("Kafka Object read pin is not active");
        }
        return count;
    }
}
