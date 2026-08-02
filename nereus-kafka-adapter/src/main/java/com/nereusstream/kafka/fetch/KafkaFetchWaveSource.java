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

package com.nereusstream.kafka.fetch;

import java.util.concurrent.CompletionStage;

/**
 * One immutable Kafka Fetch request bound to stock-owned read and event adapters.
 *
 * <p>The first wave may update stock follower-fetch state. Every later wave must be side-effect equivalent to a stock
 * delayed-fetch reread. The returned subscription must cover every partition in the request before the initial read
 * starts, so an append racing with the initial empty result cannot be lost.
 */
public interface KafkaFetchWaveSource<T> {
    CompletionStage<T> read(boolean initialWave);

    AutoCloseable subscribe(Runnable wakeup);
}
