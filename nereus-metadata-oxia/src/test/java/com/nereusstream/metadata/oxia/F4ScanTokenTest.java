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

package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class F4ScanTokenTest {
    private static final String SHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    void tokenIsScopeBoundAndResumesStrictlyAfterLastKey() {
        F4ScanToken token = new F4ScanToken(
                "cluster",
                F4ScanKind.PHYSICAL_ROOT,
                SHA256,
                "/roots/001/",
                "/roots/001/key");

        assertThat(token.cluster()).isEqualTo("cluster");
        assertThat(token.kind()).isEqualTo(F4ScanKind.PHYSICAL_ROOT);
        assertThat(token.scopeIdentitySha256()).isEqualTo(SHA256);
        assertThat(token.resumeFromInclusive()).isEqualTo("/roots/001/key\0");
    }

    @Test
    void rejectsMalformedScopeAndOutOfPrefixCursor() {
        assertThatThrownBy(() -> new F4ScanToken(
                "cluster", F4ScanKind.PHYSICAL_ROOT, "not-a-digest", "/roots/001/", "/roots/001/key"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new F4ScanToken(
                "cluster", F4ScanKind.PHYSICAL_ROOT, SHA256, "/roots/001/", "/roots/002/key"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
