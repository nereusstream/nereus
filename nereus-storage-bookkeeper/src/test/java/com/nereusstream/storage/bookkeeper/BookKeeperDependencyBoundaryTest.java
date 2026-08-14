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

package com.nereusstream.storage.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.net.URISyntaxException;
import java.nio.file.Path;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.junit.jupiter.api.Test;

class BookKeeperDependencyBoundaryTest {
    @Test
    void productionClasspathPinsBookKeeperAndExcludesProtocolAndMetadataRuntimes() throws URISyntaxException {
        Path bookKeeperLocation = Path.of(BookKeeper.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());

        assertThat(bookKeeperLocation.getFileName().toString()).isEqualTo("bookkeeper-server-4.18.0.jar");
        assertThatThrownBy(() -> Class.forName("org.apache.kafka.clients.producer.KafkaProducer"))
                .isInstanceOf(ClassNotFoundException.class);
        assertThatThrownBy(() -> Class.forName("org.apache.pulsar.client.api.PulsarClient"))
                .isInstanceOf(ClassNotFoundException.class);
        assertThatThrownBy(() -> Class.forName("io.github.oxia.client.api.OxiaClient"))
                .isInstanceOf(ClassNotFoundException.class);
    }
}
