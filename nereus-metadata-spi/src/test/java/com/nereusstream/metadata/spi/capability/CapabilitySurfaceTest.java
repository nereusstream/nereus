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

package com.nereusstream.metadata.spi.capability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class CapabilitySurfaceTest {
    private static final Set<Class<?>> CAPABILITIES = Set.of(
            TopicBindingAggregatePublisher.class,
            TopicBindingAggregateReader.class,
            PulsarTopicGenerationSelectorStore.class,
            PulsarVirtualLedgerNamespaceRegistryStore.class);

    @Test
    void capabilityInventoryContainsOnlyTheFourAcceptedInterfaces() {
        assertThat(CAPABILITIES)
                .extracting(Class::getSimpleName)
                .containsExactlyInAnyOrder(
                        "TopicBindingAggregatePublisher",
                        "TopicBindingAggregateReader",
                        "PulsarTopicGenerationSelectorStore",
                        "PulsarVirtualLedgerNamespaceRegistryStore");
        assertThat(CAPABILITIES).allMatch(Class::isInterface);
        assertThat(CAPABILITIES).allMatch(type -> Modifier.isPublic(type.getModifiers()));
    }

    @Test
    void methodsAreCapabilitySpecificAndExposeNoGenericCrudListWatchOrDelete() {
        Set<String> methodNames = CAPABILITIES.stream()
                .flatMap(type -> Arrays.stream(type.getDeclaredMethods()))
                .map(Method::getName)
                .collect(Collectors.toSet());

        assertThat(methodNames)
                .containsExactlyInAnyOrder(
                        "publishIfAbsent",
                        "readAggregate",
                        "readSelector",
                        "createSelector",
                        "compareAndSetSelector",
                        "readRegistry",
                        "createRegistry",
                        "compareAndSetRegistry")
                .noneMatch(
                        name -> Set.of("get", "put", "delete", "list", "watch").contains(name));
    }

    @Test
    void umbrellaAndChildAuthorityInterfacesDoNotExist() {
        for (String forbidden : new String[] {
            "com.nereusstream.metadata.spi.capability.MetadataStore",
            "com.nereusstream.metadata.spi.capability.TopicBindingStore",
            "com.nereusstream.metadata.spi.capability.StorageEpochStore",
            "com.nereusstream.metadata.spi.capability.VirtualLedgerAllocator"
        }) {
            assertThatThrownBy(() -> Class.forName(forbidden)).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
