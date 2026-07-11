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

package com.nereusstream.api;

/**
 * Physical storage and object-materialization profile for an L0 stream.
 *
 * <p>An enum value reserves a durable metadata name; it does not by itself imply that the current core has an
 * end-to-end writer, reader, and coordinator for that profile.
 */
public enum StorageProfile {
    /**
     * Legacy Phase 1 object-WAL profile name.
     *
     * <p>New code should prefer {@link #OBJECT_WAL_SYNC_OBJECT}.
     */
    @Deprecated
    OBJECT_WAL,

    /** BookKeeper is the only durable WAL; remote object materialization is disabled. */
    BOOKKEEPER_WAL_ONLY,

    /** BookKeeper is the WAL and object-backed visibility/materialization is completed on the append path. */
    BOOKKEEPER_WAL_SYNC_OBJECT,

    /** BookKeeper is the WAL and object-backed materialization is completed by background workers. */
    BOOKKEEPER_WAL_ASYNC_OBJECT,

    /** Object storage is the WAL and object-backed visibility/materialization is completed on the append path. */
    OBJECT_WAL_SYNC_OBJECT,

    /** Object storage is the WAL and read-optimized object materialization is completed by background workers. */
    OBJECT_WAL_ASYNC_OBJECT;

    public StorageProfile canonical() {
        return this == OBJECT_WAL ? OBJECT_WAL_SYNC_OBJECT : this;
    }

    public boolean usesBookKeeperWal() {
        return canonical() == BOOKKEEPER_WAL_ONLY
                || canonical() == BOOKKEEPER_WAL_SYNC_OBJECT
                || canonical() == BOOKKEEPER_WAL_ASYNC_OBJECT;
    }

    public boolean usesObjectWal() {
        return canonical() == OBJECT_WAL_SYNC_OBJECT || canonical() == OBJECT_WAL_ASYNC_OBJECT;
    }

    public boolean objectMaterializationEnabled() {
        return canonical() != BOOKKEEPER_WAL_ONLY;
    }

    public boolean syncObjectMaterialization() {
        return objectMaterializationEnabled() && !asyncObjectMaterialization();
    }

    public boolean asyncObjectMaterialization() {
        return canonical() == BOOKKEEPER_WAL_ASYNC_OBJECT || canonical() == OBJECT_WAL_ASYNC_OBJECT;
    }

    public DurabilityLevel defaultDurabilityLevel() {
        return syncObjectMaterialization()
                ? DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED
                : DurabilityLevel.WAL_DURABLE;
    }
}
