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

import org.junit.jupiter.api.Test;

class RuntimeDependencyIsolationTest {
    @Test
    void objectStoreLibraryDoesNotSelectTheBrokerLoggingBackend() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        assertThat(loader.getResource("org/slf4j/impl/Reload4jLoggerFactory.class"))
                .as("Hadoop's reload4j binding must not leak from the object-store library")
                .isNull();
    }
}
