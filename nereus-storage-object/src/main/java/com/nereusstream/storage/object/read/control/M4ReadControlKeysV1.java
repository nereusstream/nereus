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

package com.nereusstream.storage.object.read.control;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Canonical bounded M4 keys below one configured Object-WAL shard authority. */
public final class M4ReadControlKeysV1 {
    private static final Pattern QUALIFIED_PROTECTION =
            Pattern.compile("^(?:(?:/[A-Za-z0-9_-]+)+/)?(v2/object-wal/shards/([0-9]{10})/read-m4/[0-9a-f]{64}"
                    + "/protections/[0-9a-f]{64}-[0-9]{20})$");
    private final String prefix;

    public M4ReadControlKeysV1(int shardId, BindingIdentity binding) {
        Objects.requireNonNull(binding, "binding");
        if (shardId < 0) {
            throw new IllegalArgumentException("shard ID must be non-negative");
        }
        prefix = String.format(
                Locale.ROOT,
                "v2/object-wal/shards/%010d/read-m4/%s",
                shardId,
                binding.bindingId().digest().toHex());
    }

    public String selector() {
        return prefix + "/selector";
    }

    public String capability(long generation) {
        return prefix + "/capabilities/" + ordinal(generation);
    }

    public String terminal(long epoch) {
        return prefix + "/terminals/" + ordinal(epoch);
    }

    public String proof(long epoch) {
        return prefix + "/proofs/" + ordinal(epoch);
    }

    public String proofHead() {
        return prefix + "/proof-head";
    }

    public String protection(Sha256Digest sourceIdentity, long protectionGeneration) {
        Objects.requireNonNull(sourceIdentity, "sourceIdentity");
        return prefix + "/protections/" + sourceIdentity.toHex() + "-" + ordinal(protectionGeneration);
    }

    /** Accepts only this exact M4 protection key, relative or below a canonical Cell authority root. */
    public static boolean matchesProtectionAuthority(
            String key, BindingIdentity binding, Sha256Digest sourceIdentity, long protectionGeneration) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(sourceIdentity, "sourceIdentity");
        Matcher qualified = QUALIFIED_PROTECTION.matcher(key);
        if (!qualified.matches()) {
            return false;
        }
        try {
            int shardId = Integer.parseInt(qualified.group(2));
            return qualified
                    .group(1)
                    .equals(new M4ReadControlKeysV1(shardId, binding).protection(sourceIdentity, protectionGeneration));
        } catch (IllegalArgumentException invalidKey) {
            return false;
        }
    }

    private static String ordinal(long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("M4 key ordinal must be positive");
        }
        return String.format(Locale.ROOT, "%020d", value);
    }
}
