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

package io.nereus.metadata.oxia.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class OxiaMetadataStoreSharedContractTest {
    @Test
    void fakePassesTheSharedM7OperationContract() {
        try (FakeOxiaMetadataStore store = new FakeOxiaMetadataStore()) {
            OxiaMetadataStoreContractScenario.ContractResult result =
                    OxiaMetadataStoreContractScenario.run(store, "shared/fake/" + UUID.randomUUID());
            assertThat(result.commitVersion()).isEqualTo(1);
        }
    }
}
