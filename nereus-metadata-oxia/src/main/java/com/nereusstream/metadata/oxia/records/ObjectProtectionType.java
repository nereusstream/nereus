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

package com.nereusstream.metadata.oxia.records;

/** Durable owner/reference class protecting one physical object from deletion. */
public enum ObjectProtectionType {
    VISIBLE_GENERATION(1),
    REACHABLE_APPEND(2),
    MATERIALIZATION_SOURCE(3),
    MATERIALIZATION_OUTPUT(4),
    RECOVERY_CHECKPOINT_OBJECT(5),
    RECOVERY_CHECKPOINT_TARGET(6),
    CURSOR_SNAPSHOT_PENDING(7),
    CURSOR_SNAPSHOT_ROOT(8),
    REPAIR(9),
    CATALOG_SNAPSHOT(10),
    CATALOG_DELIVERY(11),
    RECOVERY_CHECKPOINT_PENDING(12);

    private final int wireId;

    ObjectProtectionType(int wireId) {
        this.wireId = wireId;
    }

    public int wireId() {
        return wireId;
    }

    public static ObjectProtectionType fromWireId(int wireId) {
        for (ObjectProtectionType value : values()) {
            if (value.wireId == wireId) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown object protection type wire id: " + wireId);
    }
}
