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

package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.util.Optional;
import java.util.OptionalLong;

/** Test-only access to the package-private durable materialization mapper. */
public final class KafkaCompactionTaskTestSupport {
  private KafkaCompactionTaskTestSupport() {}

  public static VersionedMaterializationTask planned(
      MaterializationTask task, long metadataVersion) {
    MaterializationTaskRecord value =
        MaterializationRecordMapper.plannedTask(task, 100).withMetadataVersion(metadataVersion);
    return versioned(value);
  }

  public static VersionedMaterializationTask cancelled(
      MaterializationTask task, long metadataVersion) {
    MaterializationTaskRecord planned = MaterializationRecordMapper.plannedTask(task, 100);
    MaterializationTaskRecord value =
        new MaterializationTaskRecord(
            planned.schemaVersion(),
            planned.taskId(),
            planned.taskSequence(),
            planned.streamId(),
            planned.readViewId(),
            planned.taskKindId(),
            planned.offsetStart(),
            planned.offsetEnd(),
            planned.sources(),
            planned.sourceSetSha256(),
            planned.policyId(),
            planned.policyVersion(),
            planned.policySha256(),
            planned.policy(),
            TaskLifecycle.CANCELLED,
            0,
            Optional.empty(),
            Optional.empty(),
            OptionalLong.empty(),
            "",
            TaskFailureClass.CLOSED.wireId(),
            "cancelled for terminal-retirement test",
            0,
            planned.createdAtMillis(),
            200,
            metadataVersion);
    return versioned(value);
  }

  private static VersionedMaterializationTask versioned(MaterializationTaskRecord value) {
    return new VersionedMaterializationTask(
        "tasks/" + value.taskId(),
        value,
        value.metadataVersion(),
        new Checksum(ChecksumType.SHA256, "c".repeat(64)));
  }
}
