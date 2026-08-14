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

package com.nereusstream.kafka.bookkeeper.commit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Immutable ordered hidden protocol-delta queue reflected by Allocated but not Readable/LEO. */
public record KafkaSpeculativeQueueV1(List<KafkaSpeculativeCommitV1> commits) {
    public KafkaSpeculativeQueueV1 {
        commits = List.copyOf(Objects.requireNonNull(commits, "commits"));
        long next = -1;
        for (KafkaSpeculativeCommitV1 commit : commits) {
            if (next >= 0 && commit.startOffset() != next) {
                throw new IllegalArgumentException("speculative commit queue is not contiguous");
            }
            next = commit.endOffsetExclusive();
        }
    }

    public static KafkaSpeculativeQueueV1 empty() {
        return new KafkaSpeculativeQueueV1(List.of());
    }

    public KafkaSpeculativeQueueV1 append(KafkaSpeculativeCommitV1 commit, long expectedStartOffset) {
        Objects.requireNonNull(commit, "commit");
        long next = commits.isEmpty()
                ? expectedStartOffset
                : commits.get(commits.size() - 1).endOffsetExclusive();
        if (commit.startOffset() != next) {
            throw new IllegalArgumentException("speculative commit does not extend the queue");
        }
        List<KafkaSpeculativeCommitV1> replacement = new ArrayList<>(commits);
        replacement.add(commit);
        return new KafkaSpeculativeQueueV1(replacement);
    }

    public KafkaSpeculativeCommitV1 head() {
        if (commits.isEmpty()) {
            throw new IllegalStateException("speculative commit queue is empty");
        }
        return commits.get(0);
    }

    public KafkaSpeculativeQueueV1 removeHead() {
        if (commits.isEmpty()) {
            throw new IllegalStateException("speculative commit queue is empty");
        }
        return new KafkaSpeculativeQueueV1(commits.subList(1, commits.size()));
    }
}
