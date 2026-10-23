/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package org.apache.kafka.common.metadata;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class PartitionChangeRecord implements ApiMessage {
    int partitionId;
    Uuid topicId;
    List<Integer> isr;
    int leader;
    List<Integer> replicas;
    List<Integer> removingReplicas;
    List<Integer> addingReplicas;
    byte leaderRecoveryState;
    List<Uuid> directories;
    List<Integer> eligibleLeaderReplicas;
    List<Integer> lastKnownElr;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("partition_id", Type.INT32, "The partition id."),
            new Field("topic_id", Type.UUID, "The unique ID of this topic."),
            TaggedFieldsSection.of(
                0, new Field("isr", CompactArrayOf.nullable(Type.INT32), "null if the ISR didn't change; the new in-sync replicas otherwise."),
                1, new Field("leader", Type.INT32, "-1 if there is now no leader; -2 if the leader didn't change; the new leader otherwise."),
                2, new Field("replicas", CompactArrayOf.nullable(Type.INT32), "null if the replicas didn't change; the new replicas otherwise."),
                3, new Field("removing_replicas", CompactArrayOf.nullable(Type.INT32), "null if the removing replicas didn't change; the new removing replicas otherwise."),
                4, new Field("adding_replicas", CompactArrayOf.nullable(Type.INT32), "null if the adding replicas didn't change; the new adding replicas otherwise."),
                5, new Field("leader_recovery_state", Type.INT8, "-1 if it didn't change; 0 if the leader was elected from the ISR or recovered from an unclean election; 1 if the leader that was elected using unclean leader election and it is still recovering.")
            )
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("partition_id", Type.INT32, "The partition id."),
            new Field("topic_id", Type.UUID, "The unique ID of this topic."),
            TaggedFieldsSection.of(
                0, new Field("isr", CompactArrayOf.nullable(Type.INT32), "null if the ISR didn't change; the new in-sync replicas otherwise."),
                1, new Field("leader", Type.INT32, "-1 if there is now no leader; -2 if the leader didn't change; the new leader otherwise."),
                2, new Field("replicas", CompactArrayOf.nullable(Type.INT32), "null if the replicas didn't change; the new replicas otherwise."),
                3, new Field("removing_replicas", CompactArrayOf.nullable(Type.INT32), "null if the removing replicas didn't change; the new removing replicas otherwise."),
                4, new Field("adding_replicas", CompactArrayOf.nullable(Type.INT32), "null if the adding replicas didn't change; the new adding replicas otherwise."),
                5, new Field("leader_recovery_state", Type.INT8, "-1 if it didn't change; 0 if the leader was elected from the ISR or recovered from an unclean election; 1 if the leader that was elected using unclean leader election and it is still recovering."),
                8, new Field("directories", CompactArrayOf.nullable(Type.UUID), "null if the log dirs didn't change; the new log directory for each replica otherwise.")
            )
        );
    
    public static final Schema SCHEMA_2 =
        new Schema(
            new Field("partition_id", Type.INT32, "The partition id."),
            new Field("topic_id", Type.UUID, "The unique ID of this topic."),
            TaggedFieldsSection.of(
                0, new Field("isr", CompactArrayOf.nullable(Type.INT32), "null if the ISR didn't change; the new in-sync replicas otherwise."),
                1, new Field("leader", Type.INT32, "-1 if there is now no leader; -2 if the leader didn't change; the new leader otherwise."),
                2, new Field("replicas", CompactArrayOf.nullable(Type.INT32), "null if the replicas didn't change; the new replicas otherwise."),
                3, new Field("removing_replicas", CompactArrayOf.nullable(Type.INT32), "null if the removing replicas didn't change; the new removing replicas otherwise."),
                4, new Field("adding_replicas", CompactArrayOf.nullable(Type.INT32), "null if the adding replicas didn't change; the new adding replicas otherwise."),
                5, new Field("leader_recovery_state", Type.INT8, "-1 if it didn't change; 0 if the leader was elected from the ISR or recovered from an unclean election; 1 if the leader that was elected using unclean leader election and it is still recovering."),
                8, new Field("directories", CompactArrayOf.nullable(Type.UUID), "null if the log dirs didn't change; the new log directory for each replica otherwise."),
                6, new Field("eligible_leader_replicas", CompactArrayOf.nullable(Type.INT32), "null if the ELR didn't change; the new eligible leader replicas otherwise."),
                7, new Field("last_known_elr", CompactArrayOf.nullable(Type.INT32), "null if the LastKnownElr didn't change; the last known eligible leader replicas otherwise.")
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 2;
    
    public PartitionChangeRecord(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public PartitionChangeRecord() {
        this.partitionId = -1;
        this.topicId = Uuid.ZERO_UUID;
        this.isr = null;
        this.leader = -2;
        this.replicas = null;
        this.removingReplicas = null;
        this.addingReplicas = null;
        this.leaderRecoveryState = (byte) -1;
        this.directories = null;
        this.eligibleLeaderReplicas = null;
        this.lastKnownElr = null;
    }
    
    @Override
    public short apiKey() {
        return 5;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 2;
    }
    
    @Override
    public final void read(Readable _readable, short _version) {
        this.partitionId = _readable.readInt();
        this.topicId = _readable.readUuid();
        {
            this.isr = null;
        }
        this.leader = -2;
        {
            this.replicas = null;
        }
        {
            this.removingReplicas = null;
        }
        {
            this.addingReplicas = null;
        }
        this.leaderRecoveryState = (byte) -1;
        {
            this.directories = null;
        }
        {
            this.eligibleLeaderReplicas = null;
        }
        {
            this.lastKnownElr = null;
        }
        this._unknownTaggedFields = null;
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; _i++) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                case 0: {
                    int arrayLength;
                    arrayLength = _readable.readUnsignedVarint() - 1;
                    if (arrayLength < 0) {
                        this.isr = null;
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<Integer> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(_readable.readInt());
                        }
                        this.isr = newCollection;
                    }
                    break;
                }
                case 1: {
                    this.leader = _readable.readInt();
                    break;
                }
                case 2: {
                    int arrayLength;
                    arrayLength = _readable.readUnsignedVarint() - 1;
                    if (arrayLength < 0) {
                        this.replicas = null;
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<Integer> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(_readable.readInt());
                        }
                        this.replicas = newCollection;
                    }
                    break;
                }
                case 3: {
                    int arrayLength;
                    arrayLength = _readable.readUnsignedVarint() - 1;
                    if (arrayLength < 0) {
                        this.removingReplicas = null;
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<Integer> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(_readable.readInt());
                        }
                        this.removingReplicas = newCollection;
                    }
                    break;
                }
                case 4: {
                    int arrayLength;
                    arrayLength = _readable.readUnsignedVarint() - 1;
                    if (arrayLength < 0) {
                        this.addingReplicas = null;
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<Integer> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(_readable.readInt());
                        }
                        this.addingReplicas = newCollection;
                    }
                    break;
                }
                case 5: {
                    this.leaderRecoveryState = _readable.readByte();
                    break;
                }
                case 8: {
                    if (_version >= 1) {
                        int arrayLength;
                        arrayLength = _readable.readUnsignedVarint() - 1;
                        if (arrayLength < 0) {
                            this.directories = null;
                        } else {
                            if (arrayLength > _readable.remaining()) {
                                throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                            }
                            ArrayList<Uuid> newCollection = new ArrayList<>(arrayLength);
                            for (int i = 0; i < arrayLength; i++) {
                                newCollection.add(_readable.readUuid());
                            }
                            this.directories = newCollection;
                        }
                        break;
                    } else {
                        throw new RuntimeException("Tag 8 is not valid for version " + _version);
                    }
                }
                case 6: {
                    if (_version >= 2) {
                        int arrayLength;
                        arrayLength = _readable.readUnsignedVarint() - 1;
                        if (arrayLength < 0) {
                            this.eligibleLeaderReplicas = null;
                        } else {
                            if (arrayLength > _readable.remaining()) {
                                throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                            }
                            ArrayList<Integer> newCollection = new ArrayList<>(arrayLength);
                            for (int i = 0; i < arrayLength; i++) {
                                newCollection.add(_readable.readInt());
                            }
                            this.eligibleLeaderReplicas = newCollection;
                        }
                        break;
                    } else {
                        throw new RuntimeException("Tag 6 is not valid for version " + _version);
                    }
                }
                case 7: {
                    if (_version >= 2) {
                        int arrayLength;
                        arrayLength = _readable.readUnsignedVarint() - 1;
                        if (arrayLength < 0) {
                            this.lastKnownElr = null;
                        } else {
                            if (arrayLength > _readable.remaining()) {
                                throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                            }
                            ArrayList<Integer> newCollection = new ArrayList<>(arrayLength);
                            for (int i = 0; i < arrayLength; i++) {
                                newCollection.add(_readable.readInt());
                            }
                            this.lastKnownElr = newCollection;
                        }
                        break;
                    } else {
                        throw new RuntimeException("Tag 7 is not valid for version " + _version);
                    }
                }
                default:
                    this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                    break;
            }
        }
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeInt(partitionId);
        _writable.writeUuid(topicId);
        if (this.isr != null) {
            _numTaggedFields++;
        }
        if (this.leader != -2) {
            _numTaggedFields++;
        }
        if (this.replicas != null) {
            _numTaggedFields++;
        }
        if (this.removingReplicas != null) {
            _numTaggedFields++;
        }
        if (this.addingReplicas != null) {
            _numTaggedFields++;
        }
        if (this.leaderRecoveryState != (byte) -1) {
            _numTaggedFields++;
        }
        if (_version >= 1) {
            if (this.directories != null) {
                _numTaggedFields++;
            }
        } else {
            if (this.directories != null) {
                throw new UnsupportedVersionException("Attempted to write a non-default directories at version " + _version);
            }
        }
        if (_version >= 2) {
            if (this.eligibleLeaderReplicas != null) {
                _numTaggedFields++;
            }
        } else {
            if (this.eligibleLeaderReplicas != null) {
                throw new UnsupportedVersionException("Attempted to write a non-default eligibleLeaderReplicas at version " + _version);
            }
        }
        if (_version >= 2) {
            if (this.lastKnownElr != null) {
                _numTaggedFields++;
            }
        } else {
            if (this.lastKnownElr != null) {
                throw new UnsupportedVersionException("Attempted to write a non-default lastKnownElr at version " + _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        if (isr != null) {
            _writable.writeUnsignedVarint(0);
            _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.isr));
            _writable.writeUnsignedVarint(isr.size() + 1);
            for (Integer isrElement : isr) {
                _writable.writeInt(isrElement);
            }
        }
        {
            if (this.leader != -2) {
                _writable.writeUnsignedVarint(1);
                _writable.writeUnsignedVarint(4);
                _writable.writeInt(leader);
            }
        }
        if (replicas != null) {
            _writable.writeUnsignedVarint(2);
            _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.replicas));
            _writable.writeUnsignedVarint(replicas.size() + 1);
            for (Integer replicasElement : replicas) {
                _writable.writeInt(replicasElement);
            }
        }
        if (removingReplicas != null) {
            _writable.writeUnsignedVarint(3);
            _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.removingReplicas));
            _writable.writeUnsignedVarint(removingReplicas.size() + 1);
            for (Integer removingReplicasElement : removingReplicas) {
                _writable.writeInt(removingReplicasElement);
            }
        }
        if (addingReplicas != null) {
            _writable.writeUnsignedVarint(4);
            _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.addingReplicas));
            _writable.writeUnsignedVarint(addingReplicas.size() + 1);
            for (Integer addingReplicasElement : addingReplicas) {
                _writable.writeInt(addingReplicasElement);
            }
        }
        {
            if (this.leaderRecoveryState != (byte) -1) {
                _writable.writeUnsignedVarint(5);
                _writable.writeUnsignedVarint(1);
                _writable.writeByte(leaderRecoveryState);
            }
        }
        if (_version >= 2) {
            if (eligibleLeaderReplicas != null) {
                _writable.writeUnsignedVarint(6);
                _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.eligibleLeaderReplicas));
                _writable.writeUnsignedVarint(eligibleLeaderReplicas.size() + 1);
                for (Integer eligibleLeaderReplicasElement : eligibleLeaderReplicas) {
                    _writable.writeInt(eligibleLeaderReplicasElement);
                }
            }
        }
        if (_version >= 2) {
            if (lastKnownElr != null) {
                _writable.writeUnsignedVarint(7);
                _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.lastKnownElr));
                _writable.writeUnsignedVarint(lastKnownElr.size() + 1);
                for (Integer lastKnownElrElement : lastKnownElr) {
                    _writable.writeInt(lastKnownElrElement);
                }
            }
        }
        if (_version >= 1) {
            if (directories != null) {
                _writable.writeUnsignedVarint(8);
                _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.directories));
                _writable.writeUnsignedVarint(directories.size() + 1);
                for (Uuid directoriesElement : directories) {
                    _writable.writeUuid(directoriesElement);
                }
            }
        }
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(4);
        _size.addBytes(16);
        if (isr == null) {
        } else {
            _numTaggedFields++;
            _size.addBytes(1);
            int _sizeBeforeArray = _size.totalSize();
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(isr.size() + 1));
            _size.addBytes(isr.size() * 4);
            int _arraySize = _size.totalSize() - _sizeBeforeArray;
            _cache.setArraySizeInBytes(isr, _arraySize);
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));
        }
        if (this.leader != -2) {
            _numTaggedFields++;
            _size.addBytes(1);
            _size.addBytes(1);
            _size.addBytes(4);
        }
        if (replicas == null) {
        } else {
            _numTaggedFields++;
            _size.addBytes(1);
            int _sizeBeforeArray = _size.totalSize();
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(replicas.size() + 1));
            _size.addBytes(replicas.size() * 4);
            int _arraySize = _size.totalSize() - _sizeBeforeArray;
            _cache.setArraySizeInBytes(replicas, _arraySize);
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));
        }
        if (removingReplicas == null) {
        } else {
            _numTaggedFields++;
            _size.addBytes(1);
            int _sizeBeforeArray = _size.totalSize();
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(removingReplicas.size() + 1));
            _size.addBytes(removingReplicas.size() * 4);
            int _arraySize = _size.totalSize() - _sizeBeforeArray;
            _cache.setArraySizeInBytes(removingReplicas, _arraySize);
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));
        }
        if (addingReplicas == null) {
        } else {
            _numTaggedFields++;
            _size.addBytes(1);
            int _sizeBeforeArray = _size.totalSize();
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(addingReplicas.size() + 1));
            _size.addBytes(addingReplicas.size() * 4);
            int _arraySize = _size.totalSize() - _sizeBeforeArray;
            _cache.setArraySizeInBytes(addingReplicas, _arraySize);
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));
        }
        if (this.leaderRecoveryState != (byte) -1) {
            _numTaggedFields++;
            _size.addBytes(1);
            _size.addBytes(1);
            _size.addBytes(1);
        }
        if (_version >= 1) {
            if (directories == null) {
            } else {
                _numTaggedFields++;
                _size.addBytes(1);
                int _sizeBeforeArray = _size.totalSize();
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(directories.size() + 1));
                _size.addBytes(directories.size() * 16);
                int _arraySize = _size.totalSize() - _sizeBeforeArray;
                _cache.setArraySizeInBytes(directories, _arraySize);
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));
            }
        }
        if (_version >= 2) {
            if (eligibleLeaderReplicas == null) {
            } else {
                _numTaggedFields++;
                _size.addBytes(1);
                int _sizeBeforeArray = _size.totalSize();
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(eligibleLeaderReplicas.size() + 1));
                _size.addBytes(eligibleLeaderReplicas.size() * 4);
                int _arraySize = _size.totalSize() - _sizeBeforeArray;
                _cache.setArraySizeInBytes(eligibleLeaderReplicas, _arraySize);
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));
            }
        }
        if (_version >= 2) {
            if (lastKnownElr == null) {
            } else {
                _numTaggedFields++;
                _size.addBytes(1);
                int _sizeBeforeArray = _size.totalSize();
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(lastKnownElr.size() + 1));
                _size.addBytes(lastKnownElr.size() * 4);
                int _arraySize = _size.totalSize() - _sizeBeforeArray;
                _cache.setArraySizeInBytes(lastKnownElr, _arraySize);
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));
            }
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                _size.addBytes(_field.size());
            }
        }
        _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PartitionChangeRecord)) return false;
        PartitionChangeRecord other = (PartitionChangeRecord) obj;
        if (partitionId != other.partitionId) return false;
        if (!this.topicId.equals(other.topicId)) return false;
        if (this.isr == null) {
            if (other.isr != null) return false;
        } else {
            if (!this.isr.equals(other.isr)) return false;
        }
        if (leader != other.leader) return false;
        if (this.replicas == null) {
            if (other.replicas != null) return false;
        } else {
            if (!this.replicas.equals(other.replicas)) return false;
        }
        if (this.removingReplicas == null) {
            if (other.removingReplicas != null) return false;
        } else {
            if (!this.removingReplicas.equals(other.removingReplicas)) return false;
        }
        if (this.addingReplicas == null) {
            if (other.addingReplicas != null) return false;
        } else {
            if (!this.addingReplicas.equals(other.addingReplicas)) return false;
        }
        if (leaderRecoveryState != other.leaderRecoveryState) return false;
        if (this.directories == null) {
            if (other.directories != null) return false;
        } else {
            if (!this.directories.equals(other.directories)) return false;
        }
        if (this.eligibleLeaderReplicas == null) {
            if (other.eligibleLeaderReplicas != null) return false;
        } else {
            if (!this.eligibleLeaderReplicas.equals(other.eligibleLeaderReplicas)) return false;
        }
        if (this.lastKnownElr == null) {
            if (other.lastKnownElr != null) return false;
        } else {
            if (!this.lastKnownElr.equals(other.lastKnownElr)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + partitionId;
        hashCode = 31 * hashCode + topicId.hashCode();
        hashCode = 31 * hashCode + (isr == null ? 0 : isr.hashCode());
        hashCode = 31 * hashCode + leader;
        hashCode = 31 * hashCode + (replicas == null ? 0 : replicas.hashCode());
        hashCode = 31 * hashCode + (removingReplicas == null ? 0 : removingReplicas.hashCode());
        hashCode = 31 * hashCode + (addingReplicas == null ? 0 : addingReplicas.hashCode());
        hashCode = 31 * hashCode + leaderRecoveryState;
        hashCode = 31 * hashCode + (directories == null ? 0 : directories.hashCode());
        hashCode = 31 * hashCode + (eligibleLeaderReplicas == null ? 0 : eligibleLeaderReplicas.hashCode());
        hashCode = 31 * hashCode + (lastKnownElr == null ? 0 : lastKnownElr.hashCode());
        return hashCode;
    }
    
    @Override
    public PartitionChangeRecord duplicate() {
        PartitionChangeRecord _duplicate = new PartitionChangeRecord();
        _duplicate.partitionId = partitionId;
        _duplicate.topicId = topicId;
        if (isr == null) {
            _duplicate.isr = null;
        } else {
            ArrayList<Integer> newIsr = new ArrayList<Integer>(isr.size());
            for (Integer _element : isr) {
                newIsr.add(_element);
            }
            _duplicate.isr = newIsr;
        }
        _duplicate.leader = leader;
        if (replicas == null) {
            _duplicate.replicas = null;
        } else {
            ArrayList<Integer> newReplicas = new ArrayList<Integer>(replicas.size());
            for (Integer _element : replicas) {
                newReplicas.add(_element);
            }
            _duplicate.replicas = newReplicas;
        }
        if (removingReplicas == null) {
            _duplicate.removingReplicas = null;
        } else {
            ArrayList<Integer> newRemovingReplicas = new ArrayList<Integer>(removingReplicas.size());
            for (Integer _element : removingReplicas) {
                newRemovingReplicas.add(_element);
            }
            _duplicate.removingReplicas = newRemovingReplicas;
        }
        if (addingReplicas == null) {
            _duplicate.addingReplicas = null;
        } else {
            ArrayList<Integer> newAddingReplicas = new ArrayList<Integer>(addingReplicas.size());
            for (Integer _element : addingReplicas) {
                newAddingReplicas.add(_element);
            }
            _duplicate.addingReplicas = newAddingReplicas;
        }
        _duplicate.leaderRecoveryState = leaderRecoveryState;
        if (directories == null) {
            _duplicate.directories = null;
        } else {
            ArrayList<Uuid> newDirectories = new ArrayList<Uuid>(directories.size());
            for (Uuid _element : directories) {
                newDirectories.add(_element);
            }
            _duplicate.directories = newDirectories;
        }
        if (eligibleLeaderReplicas == null) {
            _duplicate.eligibleLeaderReplicas = null;
        } else {
            ArrayList<Integer> newEligibleLeaderReplicas = new ArrayList<Integer>(eligibleLeaderReplicas.size());
            for (Integer _element : eligibleLeaderReplicas) {
                newEligibleLeaderReplicas.add(_element);
            }
            _duplicate.eligibleLeaderReplicas = newEligibleLeaderReplicas;
        }
        if (lastKnownElr == null) {
            _duplicate.lastKnownElr = null;
        } else {
            ArrayList<Integer> newLastKnownElr = new ArrayList<Integer>(lastKnownElr.size());
            for (Integer _element : lastKnownElr) {
                newLastKnownElr.add(_element);
            }
            _duplicate.lastKnownElr = newLastKnownElr;
        }
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "PartitionChangeRecord("
            + "partitionId=" + partitionId
            + ", topicId=" + topicId.toString()
            + ", isr=" + ((isr == null) ? "null" : MessageUtil.deepToString(isr.iterator()))
            + ", leader=" + leader
            + ", replicas=" + ((replicas == null) ? "null" : MessageUtil.deepToString(replicas.iterator()))
            + ", removingReplicas=" + ((removingReplicas == null) ? "null" : MessageUtil.deepToString(removingReplicas.iterator()))
            + ", addingReplicas=" + ((addingReplicas == null) ? "null" : MessageUtil.deepToString(addingReplicas.iterator()))
            + ", leaderRecoveryState=" + leaderRecoveryState
            + ", directories=" + ((directories == null) ? "null" : MessageUtil.deepToString(directories.iterator()))
            + ", eligibleLeaderReplicas=" + ((eligibleLeaderReplicas == null) ? "null" : MessageUtil.deepToString(eligibleLeaderReplicas.iterator()))
            + ", lastKnownElr=" + ((lastKnownElr == null) ? "null" : MessageUtil.deepToString(lastKnownElr.iterator()))
            + ")";
    }
    
    public int partitionId() {
        return this.partitionId;
    }
    
    public Uuid topicId() {
        return this.topicId;
    }
    
    public List<Integer> isr() {
        return this.isr;
    }
    
    public int leader() {
        return this.leader;
    }
    
    public List<Integer> replicas() {
        return this.replicas;
    }
    
    public List<Integer> removingReplicas() {
        return this.removingReplicas;
    }
    
    public List<Integer> addingReplicas() {
        return this.addingReplicas;
    }
    
    public byte leaderRecoveryState() {
        return this.leaderRecoveryState;
    }
    
    public List<Uuid> directories() {
        return this.directories;
    }
    
    public List<Integer> eligibleLeaderReplicas() {
        return this.eligibleLeaderReplicas;
    }
    
    public List<Integer> lastKnownElr() {
        return this.lastKnownElr;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public PartitionChangeRecord setPartitionId(int v) {
        this.partitionId = v;
        return this;
    }
    
    public PartitionChangeRecord setTopicId(Uuid v) {
        this.topicId = v;
        return this;
    }
    
    public PartitionChangeRecord setIsr(List<Integer> v) {
        this.isr = v;
        return this;
    }
    
    public PartitionChangeRecord setLeader(int v) {
        this.leader = v;
        return this;
    }
    
    public PartitionChangeRecord setReplicas(List<Integer> v) {
        this.replicas = v;
        return this;
    }
    
    public PartitionChangeRecord setRemovingReplicas(List<Integer> v) {
        this.removingReplicas = v;
        return this;
    }
    
    public PartitionChangeRecord setAddingReplicas(List<Integer> v) {
        this.addingReplicas = v;
        return this;
    }
    
    public PartitionChangeRecord setLeaderRecoveryState(byte v) {
        this.leaderRecoveryState = v;
        return this;
    }
    
    public PartitionChangeRecord setDirectories(List<Uuid> v) {
        this.directories = v;
        return this;
    }
    
    public PartitionChangeRecord setEligibleLeaderReplicas(List<Integer> v) {
        this.eligibleLeaderReplicas = v;
        return this;
    }
    
    public PartitionChangeRecord setLastKnownElr(List<Integer> v) {
        this.lastKnownElr = v;
        return this;
    }
}
