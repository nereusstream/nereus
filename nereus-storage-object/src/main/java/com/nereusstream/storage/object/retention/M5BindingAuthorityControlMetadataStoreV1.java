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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import java.util.Objects;
import java.util.Optional;

/**
 * Makes the M5 authority envelope transparent to M4 selector readers and writers.
 *
 * <p>Only the exact selector key is projected. Every other M4 key is delegated byte-for-byte.
 */
public final class M5BindingAuthorityControlMetadataStoreV1 implements CanonicalControlMetadataStore {
    private final CanonicalControlMetadataStore delegate;
    private final String selectorKey;

    public M5BindingAuthorityControlMetadataStoreV1(CanonicalControlMetadataStore delegate, String selectorKey) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.selectorKey = requireKey(selectorKey);
    }

    @Override
    public Optional<CanonicalBytes> get(String key) {
        Optional<CanonicalBytes> stored = delegate.get(key);
        if (!selectorKey.equals(key) || stored.isEmpty()) {
            return stored;
        }
        CanonicalBytes exact = stored.orElseThrow();
        if (!M5BindingAuthorityCodecV1.isAuthorityValue(exact)) {
            M4ReadControlCodecV1.decodeSelector(exact);
            return stored;
        }
        BindingReadSelector projection =
                M5BindingAuthorityCodecV1.decodeAuthority(exact).selectorProjection();
        return Optional.of(M4ReadControlCodecV1.encodeSelector(projection));
    }

    @Override
    public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes exactValue) {
        if (!selectorKey.equals(key)) {
            return delegate.putIfAbsent(key, exactValue);
        }
        BindingReadSelector selector = M4ReadControlCodecV1.decodeSelector(exactValue);
        CanonicalBytes authority =
                M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(selector));
        return delegate.putIfAbsent(key, authority);
    }

    @Override
    public ControlMutationOutcome compareAndSet(
            String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes exactCandidate) {
        if (!selectorKey.equals(key)) {
            return delegate.compareAndSet(key, exactExpected, exactCandidate);
        }
        Optional<CanonicalBytes> expected = Objects.requireNonNull(exactExpected, "exactExpected");
        BindingReadSelector candidate = M4ReadControlCodecV1.decodeSelector(exactCandidate);
        if (expected.isEmpty()) {
            return delegate.compareAndSet(
                    key,
                    Optional.empty(),
                    M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(candidate)));
        }
        BindingReadSelector expectedSelector = M4ReadControlCodecV1.decodeSelector(expected.orElseThrow());
        Optional<CanonicalBytes> raw = delegate.get(key);
        if (raw.isEmpty()) {
            return ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }
        CanonicalBytes rawExpected = raw.orElseThrow();
        BindingReadSelector currentProjection = M5BindingAuthorityCodecV1.projectSelector(rawExpected);
        if (!currentProjection.equals(expectedSelector)) {
            return ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }
        BindingRetirementAuthorityV1 successor;
        if (M5BindingAuthorityCodecV1.isAuthorityValue(rawExpected)) {
            BindingRetirementAuthorityV1 current = M5BindingAuthorityCodecV1.decodeAuthority(rawExpected);
            if (current.state() == M5BindingAuthorityRecordsV1.BindingAuthorityStateV1.REFERENCE_SCAN_FENCED_V1) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            successor = M5BindingAuthorityCodecV1.selectorSuccessor(current, candidate);
        } else {
            successor = M5BindingAuthorityCodecV1.migrateLegacyWithSuccessor(rawExpected, candidate);
        }
        return delegate.compareAndSet(
                key, Optional.of(rawExpected), M5BindingAuthorityCodecV1.encodeAuthority(successor));
    }

    /** Fail before M4 admission would exceed the complete authority-cell value cap. */
    public void requireSelectorCapacity(BindingReadSelector candidate) {
        Objects.requireNonNull(candidate, "candidate");
        Optional<CanonicalBytes> raw = delegate.get(selectorKey);
        BindingRetirementAuthorityV1 projected;
        if (raw.isEmpty()) {
            projected = M5BindingAuthorityCodecV1.initial(candidate);
        } else if (M5BindingAuthorityCodecV1.isAuthorityValue(raw.orElseThrow())) {
            BindingRetirementAuthorityV1 current = M5BindingAuthorityCodecV1.decodeAuthority(raw.orElseThrow());
            if (current.state() == M5BindingAuthorityRecordsV1.BindingAuthorityStateV1.REFERENCE_SCAN_FENCED_V1) {
                return;
            }
            projected = M5BindingAuthorityCodecV1.selectorSuccessor(current, candidate);
        } else {
            projected = M5BindingAuthorityCodecV1.migrateLegacyWithSuccessor(raw.orElseThrow(), candidate);
        }
        M5BindingAuthorityCodecV1.encodeAuthority(projected);
    }

    private static String requireKey(String value) {
        Objects.requireNonNull(value, "selectorKey");
        if (value.isBlank()) {
            throw new IllegalArgumentException("selector key must not be blank");
        }
        return value;
    }
}
