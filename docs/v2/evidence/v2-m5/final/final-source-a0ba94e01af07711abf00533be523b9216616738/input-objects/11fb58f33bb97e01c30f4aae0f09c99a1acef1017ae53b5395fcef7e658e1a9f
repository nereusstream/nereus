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

package org.apache.kafka.common.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.VoteRequestData.*;

public class VoteRequestDataJsonConverter {
    public static VoteRequestData read(JsonNode _node, short _version) {
        VoteRequestData _object = new VoteRequestData();
        JsonNode _clusterIdNode = _node.get("clusterId");
        if (_clusterIdNode == null) {
            throw new RuntimeException("VoteRequestData: unable to locate field 'clusterId', which is mandatory in version " + _version);
        } else {
            if (_clusterIdNode.isNull()) {
                _object.clusterId = null;
            } else {
                if (!_clusterIdNode.isTextual()) {
                    throw new RuntimeException("VoteRequestData expected a string type, but got " + _node.getNodeType());
                }
                _object.clusterId = _clusterIdNode.asText();
            }
        }
        JsonNode _voterIdNode = _node.get("voterId");
        if (_voterIdNode == null) {
            if (_version >= 1) {
                throw new RuntimeException("VoteRequestData: unable to locate field 'voterId', which is mandatory in version " + _version);
            } else {
                _object.voterId = -1;
            }
        } else {
            _object.voterId = MessageUtil.jsonNodeToInt(_voterIdNode, "VoteRequestData");
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("VoteRequestData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("VoteRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<TopicData> _collection = new ArrayList<TopicData>(_topicsNode.size());
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(TopicDataJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(VoteRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        if (_object.clusterId == null) {
            _node.set("clusterId", NullNode.instance);
        } else {
            _node.set("clusterId", new TextNode(_object.clusterId));
        }
        if (_version >= 1) {
            _node.set("voterId", new IntNode(_object.voterId));
        }
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (TopicData _element : _object.topics) {
            _topicsArray.add(TopicDataJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("topics", _topicsArray);
        return _node;
    }
    public static JsonNode write(VoteRequestData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class PartitionDataJsonConverter {
        public static PartitionData read(JsonNode _node, short _version) {
            PartitionData _object = new PartitionData();
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "PartitionData");
            }
            JsonNode _replicaEpochNode = _node.get("replicaEpoch");
            if (_replicaEpochNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'replicaEpoch', which is mandatory in version " + _version);
            } else {
                _object.replicaEpoch = MessageUtil.jsonNodeToInt(_replicaEpochNode, "PartitionData");
            }
            JsonNode _replicaIdNode = _node.get("replicaId");
            if (_replicaIdNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'replicaId', which is mandatory in version " + _version);
            } else {
                _object.replicaId = MessageUtil.jsonNodeToInt(_replicaIdNode, "PartitionData");
            }
            JsonNode _replicaDirectoryIdNode = _node.get("replicaDirectoryId");
            if (_replicaDirectoryIdNode == null) {
                if (_version >= 1) {
                    throw new RuntimeException("PartitionData: unable to locate field 'replicaDirectoryId', which is mandatory in version " + _version);
                } else {
                    _object.replicaDirectoryId = Uuid.ZERO_UUID;
                }
            } else {
                if (!_replicaDirectoryIdNode.isTextual()) {
                    throw new RuntimeException("PartitionData expected a JSON string type, but got " + _node.getNodeType());
                }
                _object.replicaDirectoryId = Uuid.fromString(_replicaDirectoryIdNode.asText());
            }
            JsonNode _voterDirectoryIdNode = _node.get("voterDirectoryId");
            if (_voterDirectoryIdNode == null) {
                if (_version >= 1) {
                    throw new RuntimeException("PartitionData: unable to locate field 'voterDirectoryId', which is mandatory in version " + _version);
                } else {
                    _object.voterDirectoryId = Uuid.ZERO_UUID;
                }
            } else {
                if (!_voterDirectoryIdNode.isTextual()) {
                    throw new RuntimeException("PartitionData expected a JSON string type, but got " + _node.getNodeType());
                }
                _object.voterDirectoryId = Uuid.fromString(_voterDirectoryIdNode.asText());
            }
            JsonNode _lastOffsetEpochNode = _node.get("lastOffsetEpoch");
            if (_lastOffsetEpochNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'lastOffsetEpoch', which is mandatory in version " + _version);
            } else {
                _object.lastOffsetEpoch = MessageUtil.jsonNodeToInt(_lastOffsetEpochNode, "PartitionData");
            }
            JsonNode _lastOffsetNode = _node.get("lastOffset");
            if (_lastOffsetNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'lastOffset', which is mandatory in version " + _version);
            } else {
                _object.lastOffset = MessageUtil.jsonNodeToLong(_lastOffsetNode, "PartitionData");
            }
            JsonNode _preVoteNode = _node.get("preVote");
            if (_preVoteNode == null) {
                if (_version >= 2) {
                    throw new RuntimeException("PartitionData: unable to locate field 'preVote', which is mandatory in version " + _version);
                } else {
                    _object.preVote = false;
                }
            } else {
                if (!_preVoteNode.isBoolean()) {
                    throw new RuntimeException("PartitionData expected Boolean type, but got " + _node.getNodeType());
                }
                _object.preVote = _preVoteNode.asBoolean();
            }
            return _object;
        }
        public static JsonNode write(PartitionData _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            _node.set("replicaEpoch", new IntNode(_object.replicaEpoch));
            _node.set("replicaId", new IntNode(_object.replicaId));
            if (_version >= 1) {
                _node.set("replicaDirectoryId", new TextNode(_object.replicaDirectoryId.toString()));
            }
            if (_version >= 1) {
                _node.set("voterDirectoryId", new TextNode(_object.voterDirectoryId.toString()));
            }
            _node.set("lastOffsetEpoch", new IntNode(_object.lastOffsetEpoch));
            _node.set("lastOffset", new LongNode(_object.lastOffset));
            if (_version >= 2) {
                _node.set("preVote", BooleanNode.valueOf(_object.preVote));
            } else {
                if (_object.preVote) {
                    throw new UnsupportedVersionException("Attempted to write a non-default preVote at version " + _version);
                }
            }
            return _node;
        }
        public static JsonNode write(PartitionData _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class TopicDataJsonConverter {
        public static TopicData read(JsonNode _node, short _version) {
            TopicData _object = new TopicData();
            JsonNode _topicNameNode = _node.get("topicName");
            if (_topicNameNode == null) {
                throw new RuntimeException("TopicData: unable to locate field 'topicName', which is mandatory in version " + _version);
            } else {
                if (!_topicNameNode.isTextual()) {
                    throw new RuntimeException("TopicData expected a string type, but got " + _node.getNodeType());
                }
                _object.topicName = _topicNameNode.asText();
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("TopicData: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("TopicData expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<PartitionData> _collection = new ArrayList<PartitionData>(_partitionsNode.size());
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(PartitionDataJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(TopicData _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("topicName", new TextNode(_object.topicName));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (PartitionData _element : _object.partitions) {
                _partitionsArray.add(PartitionDataJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
        public static JsonNode write(TopicData _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
