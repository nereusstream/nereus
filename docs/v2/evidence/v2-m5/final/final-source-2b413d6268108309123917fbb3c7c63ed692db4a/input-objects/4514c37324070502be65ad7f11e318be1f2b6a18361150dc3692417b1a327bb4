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
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.*;

public class DescribeTopicPartitionsResponseDataJsonConverter {
    public static DescribeTopicPartitionsResponseData read(JsonNode _node, short _version) {
        DescribeTopicPartitionsResponseData _object = new DescribeTopicPartitionsResponseData();
        JsonNode _throttleTimeMsNode = _node.get("throttleTimeMs");
        if (_throttleTimeMsNode == null) {
            throw new RuntimeException("DescribeTopicPartitionsResponseData: unable to locate field 'throttleTimeMs', which is mandatory in version " + _version);
        } else {
            _object.throttleTimeMs = MessageUtil.jsonNodeToInt(_throttleTimeMsNode, "DescribeTopicPartitionsResponseData");
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("DescribeTopicPartitionsResponseData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("DescribeTopicPartitionsResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            DescribeTopicPartitionsResponseTopicCollection _collection = new DescribeTopicPartitionsResponseTopicCollection(_topicsNode.size());
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(DescribeTopicPartitionsResponseTopicJsonConverter.read(_element, _version));
            }
        }
        JsonNode _nextCursorNode = _node.get("nextCursor");
        if (_nextCursorNode == null) {
            throw new RuntimeException("DescribeTopicPartitionsResponseData: unable to locate field 'nextCursor', which is mandatory in version " + _version);
        } else {
            if (_nextCursorNode.isNull()) {
                _object.nextCursor = null;
            } else {
                _object.nextCursor = CursorJsonConverter.read(_nextCursorNode, _version);
            }
        }
        return _object;
    }
    public static JsonNode write(DescribeTopicPartitionsResponseData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("throttleTimeMs", new IntNode(_object.throttleTimeMs));
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (DescribeTopicPartitionsResponseTopic _element : _object.topics) {
            _topicsArray.add(DescribeTopicPartitionsResponseTopicJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("topics", _topicsArray);
        if (_object.nextCursor == null) {
            _node.set("nextCursor", NullNode.instance);
        } else {
            _node.set("nextCursor", CursorJsonConverter.write(_object.nextCursor, _version, _serializeRecords));
        }
        return _node;
    }
    public static JsonNode write(DescribeTopicPartitionsResponseData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class CursorJsonConverter {
        public static Cursor read(JsonNode _node, short _version) {
            Cursor _object = new Cursor();
            JsonNode _topicNameNode = _node.get("topicName");
            if (_topicNameNode == null) {
                throw new RuntimeException("Cursor: unable to locate field 'topicName', which is mandatory in version " + _version);
            } else {
                if (!_topicNameNode.isTextual()) {
                    throw new RuntimeException("Cursor expected a string type, but got " + _node.getNodeType());
                }
                _object.topicName = _topicNameNode.asText();
            }
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("Cursor: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "Cursor");
            }
            return _object;
        }
        public static JsonNode write(Cursor _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("topicName", new TextNode(_object.topicName));
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            return _node;
        }
        public static JsonNode write(Cursor _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class DescribeTopicPartitionsResponsePartitionJsonConverter {
        public static DescribeTopicPartitionsResponsePartition read(JsonNode _node, short _version) {
            DescribeTopicPartitionsResponsePartition _object = new DescribeTopicPartitionsResponsePartition();
            JsonNode _errorCodeNode = _node.get("errorCode");
            if (_errorCodeNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponsePartition: unable to locate field 'errorCode', which is mandatory in version " + _version);
            } else {
                _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "DescribeTopicPartitionsResponsePartition");
            }
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponsePartition: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "DescribeTopicPartitionsResponsePartition");
            }
            JsonNode _leaderIdNode = _node.get("leaderId");
            if (_leaderIdNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponsePartition: unable to locate field 'leaderId', which is mandatory in version " + _version);
            } else {
                _object.leaderId = MessageUtil.jsonNodeToInt(_leaderIdNode, "DescribeTopicPartitionsResponsePartition");
            }
            JsonNode _leaderEpochNode = _node.get("leaderEpoch");
            if (_leaderEpochNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponsePartition: unable to locate field 'leaderEpoch', which is mandatory in version " + _version);
            } else {
                _object.leaderEpoch = MessageUtil.jsonNodeToInt(_leaderEpochNode, "DescribeTopicPartitionsResponsePartition");
            }
            JsonNode _replicaNodesNode = _node.get("replicaNodes");
            if (_replicaNodesNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponsePartition: unable to locate field 'replicaNodes', which is mandatory in version " + _version);
            } else {
                if (!_replicaNodesNode.isArray()) {
                    throw new RuntimeException("DescribeTopicPartitionsResponsePartition expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<Integer> _collection = new ArrayList<Integer>(_replicaNodesNode.size());
                _object.replicaNodes = _collection;
                for (JsonNode _element : _replicaNodesNode) {
                    _collection.add(MessageUtil.jsonNodeToInt(_element, "DescribeTopicPartitionsResponsePartition element"));
                }
            }
            JsonNode _isrNodesNode = _node.get("isrNodes");
            if (_isrNodesNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponsePartition: unable to locate field 'isrNodes', which is mandatory in version " + _version);
            } else {
                if (!_isrNodesNode.isArray()) {
                    throw new RuntimeException("DescribeTopicPartitionsResponsePartition expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<Integer> _collection = new ArrayList<Integer>(_isrNodesNode.size());
                _object.isrNodes = _collection;
                for (JsonNode _element : _isrNodesNode) {
                    _collection.add(MessageUtil.jsonNodeToInt(_element, "DescribeTopicPartitionsResponsePartition element"));
                }
            }
            JsonNode _eligibleLeaderReplicasNode = _node.get("eligibleLeaderReplicas");
            if (_eligibleLeaderReplicasNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponsePartition: unable to locate field 'eligibleLeaderReplicas', which is mandatory in version " + _version);
            } else {
                if (_eligibleLeaderReplicasNode.isNull()) {
                    _object.eligibleLeaderReplicas = null;
                } else {
                    if (!_eligibleLeaderReplicasNode.isArray()) {
                        throw new RuntimeException("DescribeTopicPartitionsResponsePartition expected a JSON array, but got " + _node.getNodeType());
                    }
                    ArrayList<Integer> _collection = new ArrayList<Integer>(_eligibleLeaderReplicasNode.size());
                    _object.eligibleLeaderReplicas = _collection;
                    for (JsonNode _element : _eligibleLeaderReplicasNode) {
                        _collection.add(MessageUtil.jsonNodeToInt(_element, "DescribeTopicPartitionsResponsePartition element"));
                    }
                }
            }
            JsonNode _lastKnownElrNode = _node.get("lastKnownElr");
            if (_lastKnownElrNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponsePartition: unable to locate field 'lastKnownElr', which is mandatory in version " + _version);
            } else {
                if (_lastKnownElrNode.isNull()) {
                    _object.lastKnownElr = null;
                } else {
                    if (!_lastKnownElrNode.isArray()) {
                        throw new RuntimeException("DescribeTopicPartitionsResponsePartition expected a JSON array, but got " + _node.getNodeType());
                    }
                    ArrayList<Integer> _collection = new ArrayList<Integer>(_lastKnownElrNode.size());
                    _object.lastKnownElr = _collection;
                    for (JsonNode _element : _lastKnownElrNode) {
                        _collection.add(MessageUtil.jsonNodeToInt(_element, "DescribeTopicPartitionsResponsePartition element"));
                    }
                }
            }
            JsonNode _offlineReplicasNode = _node.get("offlineReplicas");
            if (_offlineReplicasNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponsePartition: unable to locate field 'offlineReplicas', which is mandatory in version " + _version);
            } else {
                if (!_offlineReplicasNode.isArray()) {
                    throw new RuntimeException("DescribeTopicPartitionsResponsePartition expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<Integer> _collection = new ArrayList<Integer>(_offlineReplicasNode.size());
                _object.offlineReplicas = _collection;
                for (JsonNode _element : _offlineReplicasNode) {
                    _collection.add(MessageUtil.jsonNodeToInt(_element, "DescribeTopicPartitionsResponsePartition element"));
                }
            }
            return _object;
        }
        public static JsonNode write(DescribeTopicPartitionsResponsePartition _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("errorCode", new ShortNode(_object.errorCode));
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            _node.set("leaderId", new IntNode(_object.leaderId));
            _node.set("leaderEpoch", new IntNode(_object.leaderEpoch));
            ArrayNode _replicaNodesArray = new ArrayNode(JsonNodeFactory.instance);
            for (Integer _element : _object.replicaNodes) {
                _replicaNodesArray.add(new IntNode(_element));
            }
            _node.set("replicaNodes", _replicaNodesArray);
            ArrayNode _isrNodesArray = new ArrayNode(JsonNodeFactory.instance);
            for (Integer _element : _object.isrNodes) {
                _isrNodesArray.add(new IntNode(_element));
            }
            _node.set("isrNodes", _isrNodesArray);
            if (_object.eligibleLeaderReplicas == null) {
                _node.set("eligibleLeaderReplicas", NullNode.instance);
            } else {
                ArrayNode _eligibleLeaderReplicasArray = new ArrayNode(JsonNodeFactory.instance);
                for (Integer _element : _object.eligibleLeaderReplicas) {
                    _eligibleLeaderReplicasArray.add(new IntNode(_element));
                }
                _node.set("eligibleLeaderReplicas", _eligibleLeaderReplicasArray);
            }
            if (_object.lastKnownElr == null) {
                _node.set("lastKnownElr", NullNode.instance);
            } else {
                ArrayNode _lastKnownElrArray = new ArrayNode(JsonNodeFactory.instance);
                for (Integer _element : _object.lastKnownElr) {
                    _lastKnownElrArray.add(new IntNode(_element));
                }
                _node.set("lastKnownElr", _lastKnownElrArray);
            }
            ArrayNode _offlineReplicasArray = new ArrayNode(JsonNodeFactory.instance);
            for (Integer _element : _object.offlineReplicas) {
                _offlineReplicasArray.add(new IntNode(_element));
            }
            _node.set("offlineReplicas", _offlineReplicasArray);
            return _node;
        }
        public static JsonNode write(DescribeTopicPartitionsResponsePartition _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class DescribeTopicPartitionsResponseTopicJsonConverter {
        public static DescribeTopicPartitionsResponseTopic read(JsonNode _node, short _version) {
            DescribeTopicPartitionsResponseTopic _object = new DescribeTopicPartitionsResponseTopic();
            JsonNode _errorCodeNode = _node.get("errorCode");
            if (_errorCodeNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponseTopic: unable to locate field 'errorCode', which is mandatory in version " + _version);
            } else {
                _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "DescribeTopicPartitionsResponseTopic");
            }
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponseTopic: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (_nameNode.isNull()) {
                    _object.name = null;
                } else {
                    if (!_nameNode.isTextual()) {
                        throw new RuntimeException("DescribeTopicPartitionsResponseTopic expected a string type, but got " + _node.getNodeType());
                    }
                    _object.name = _nameNode.asText();
                }
            }
            JsonNode _topicIdNode = _node.get("topicId");
            if (_topicIdNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponseTopic: unable to locate field 'topicId', which is mandatory in version " + _version);
            } else {
                if (!_topicIdNode.isTextual()) {
                    throw new RuntimeException("DescribeTopicPartitionsResponseTopic expected a JSON string type, but got " + _node.getNodeType());
                }
                _object.topicId = Uuid.fromString(_topicIdNode.asText());
            }
            JsonNode _isInternalNode = _node.get("isInternal");
            if (_isInternalNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponseTopic: unable to locate field 'isInternal', which is mandatory in version " + _version);
            } else {
                if (!_isInternalNode.isBoolean()) {
                    throw new RuntimeException("DescribeTopicPartitionsResponseTopic expected Boolean type, but got " + _node.getNodeType());
                }
                _object.isInternal = _isInternalNode.asBoolean();
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponseTopic: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("DescribeTopicPartitionsResponseTopic expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<DescribeTopicPartitionsResponsePartition> _collection = new ArrayList<DescribeTopicPartitionsResponsePartition>(_partitionsNode.size());
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(DescribeTopicPartitionsResponsePartitionJsonConverter.read(_element, _version));
                }
            }
            JsonNode _topicAuthorizedOperationsNode = _node.get("topicAuthorizedOperations");
            if (_topicAuthorizedOperationsNode == null) {
                throw new RuntimeException("DescribeTopicPartitionsResponseTopic: unable to locate field 'topicAuthorizedOperations', which is mandatory in version " + _version);
            } else {
                _object.topicAuthorizedOperations = MessageUtil.jsonNodeToInt(_topicAuthorizedOperationsNode, "DescribeTopicPartitionsResponseTopic");
            }
            return _object;
        }
        public static JsonNode write(DescribeTopicPartitionsResponseTopic _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("errorCode", new ShortNode(_object.errorCode));
            if (_object.name == null) {
                _node.set("name", NullNode.instance);
            } else {
                _node.set("name", new TextNode(_object.name));
            }
            _node.set("topicId", new TextNode(_object.topicId.toString()));
            _node.set("isInternal", BooleanNode.valueOf(_object.isInternal));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (DescribeTopicPartitionsResponsePartition _element : _object.partitions) {
                _partitionsArray.add(DescribeTopicPartitionsResponsePartitionJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("partitions", _partitionsArray);
            _node.set("topicAuthorizedOperations", new IntNode(_object.topicAuthorizedOperations));
            return _node;
        }
        public static JsonNode write(DescribeTopicPartitionsResponseTopic _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
