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
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.DescribeQuorumResponseData.*;

public class DescribeQuorumResponseDataJsonConverter {
    public static DescribeQuorumResponseData read(JsonNode _node, short _version) {
        DescribeQuorumResponseData _object = new DescribeQuorumResponseData();
        JsonNode _errorCodeNode = _node.get("errorCode");
        if (_errorCodeNode == null) {
            throw new RuntimeException("DescribeQuorumResponseData: unable to locate field 'errorCode', which is mandatory in version " + _version);
        } else {
            _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "DescribeQuorumResponseData");
        }
        JsonNode _errorMessageNode = _node.get("errorMessage");
        if (_errorMessageNode == null) {
            if (_version >= 2) {
                throw new RuntimeException("DescribeQuorumResponseData: unable to locate field 'errorMessage', which is mandatory in version " + _version);
            } else {
                _object.errorMessage = "";
            }
        } else {
            if (_errorMessageNode.isNull()) {
                _object.errorMessage = null;
            } else {
                if (!_errorMessageNode.isTextual()) {
                    throw new RuntimeException("DescribeQuorumResponseData expected a string type, but got " + _node.getNodeType());
                }
                _object.errorMessage = _errorMessageNode.asText();
            }
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("DescribeQuorumResponseData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("DescribeQuorumResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<TopicData> _collection = new ArrayList<TopicData>(_topicsNode.size());
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(TopicDataJsonConverter.read(_element, _version));
            }
        }
        JsonNode _nodesNode = _node.get("nodes");
        if (_nodesNode == null) {
            if (_version >= 2) {
                throw new RuntimeException("DescribeQuorumResponseData: unable to locate field 'nodes', which is mandatory in version " + _version);
            } else {
                _object.nodes = new NodeCollection(0);
            }
        } else {
            if (!_nodesNode.isArray()) {
                throw new RuntimeException("DescribeQuorumResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            NodeCollection _collection = new NodeCollection(_nodesNode.size());
            _object.nodes = _collection;
            for (JsonNode _element : _nodesNode) {
                _collection.add(NodeJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(DescribeQuorumResponseData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("errorCode", new ShortNode(_object.errorCode));
        if (_version >= 2) {
            if (_object.errorMessage == null) {
                _node.set("errorMessage", NullNode.instance);
            } else {
                _node.set("errorMessage", new TextNode(_object.errorMessage));
            }
        }
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (TopicData _element : _object.topics) {
            _topicsArray.add(TopicDataJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("topics", _topicsArray);
        if (_version >= 2) {
            ArrayNode _nodesArray = new ArrayNode(JsonNodeFactory.instance);
            for (Node _element : _object.nodes) {
                _nodesArray.add(NodeJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("nodes", _nodesArray);
        } else {
            if (!_object.nodes.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default nodes at version " + _version);
            }
        }
        return _node;
    }
    public static JsonNode write(DescribeQuorumResponseData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class ListenerJsonConverter {
        public static Listener read(JsonNode _node, short _version) {
            Listener _object = new Listener();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("Listener: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("Listener expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _hostNode = _node.get("host");
            if (_hostNode == null) {
                throw new RuntimeException("Listener: unable to locate field 'host', which is mandatory in version " + _version);
            } else {
                if (!_hostNode.isTextual()) {
                    throw new RuntimeException("Listener expected a string type, but got " + _node.getNodeType());
                }
                _object.host = _hostNode.asText();
            }
            JsonNode _portNode = _node.get("port");
            if (_portNode == null) {
                throw new RuntimeException("Listener: unable to locate field 'port', which is mandatory in version " + _version);
            } else {
                _object.port = MessageUtil.jsonNodeToUnsignedShort(_portNode, "Listener");
            }
            return _object;
        }
        public static JsonNode write(Listener _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            _node.set("host", new TextNode(_object.host));
            _node.set("port", new IntNode(_object.port));
            return _node;
        }
        public static JsonNode write(Listener _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class NodeJsonConverter {
        public static Node read(JsonNode _node, short _version) {
            Node _object = new Node();
            if (_version < 2) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of Node");
            }
            JsonNode _nodeIdNode = _node.get("nodeId");
            if (_nodeIdNode == null) {
                throw new RuntimeException("Node: unable to locate field 'nodeId', which is mandatory in version " + _version);
            } else {
                _object.nodeId = MessageUtil.jsonNodeToInt(_nodeIdNode, "Node");
            }
            JsonNode _listenersNode = _node.get("listeners");
            if (_listenersNode == null) {
                throw new RuntimeException("Node: unable to locate field 'listeners', which is mandatory in version " + _version);
            } else {
                if (!_listenersNode.isArray()) {
                    throw new RuntimeException("Node expected a JSON array, but got " + _node.getNodeType());
                }
                ListenerCollection _collection = new ListenerCollection(_listenersNode.size());
                _object.listeners = _collection;
                for (JsonNode _element : _listenersNode) {
                    _collection.add(ListenerJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(Node _object, short _version, boolean _serializeRecords) {
            if (_version < 2) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of Node");
            }
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("nodeId", new IntNode(_object.nodeId));
            ArrayNode _listenersArray = new ArrayNode(JsonNodeFactory.instance);
            for (Listener _element : _object.listeners) {
                _listenersArray.add(ListenerJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("listeners", _listenersArray);
            return _node;
        }
        public static JsonNode write(Node _object, short _version) {
            return write(_object, _version, true);
        }
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
            JsonNode _errorCodeNode = _node.get("errorCode");
            if (_errorCodeNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'errorCode', which is mandatory in version " + _version);
            } else {
                _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "PartitionData");
            }
            JsonNode _errorMessageNode = _node.get("errorMessage");
            if (_errorMessageNode == null) {
                if (_version >= 2) {
                    throw new RuntimeException("PartitionData: unable to locate field 'errorMessage', which is mandatory in version " + _version);
                } else {
                    _object.errorMessage = "";
                }
            } else {
                if (_errorMessageNode.isNull()) {
                    _object.errorMessage = null;
                } else {
                    if (!_errorMessageNode.isTextual()) {
                        throw new RuntimeException("PartitionData expected a string type, but got " + _node.getNodeType());
                    }
                    _object.errorMessage = _errorMessageNode.asText();
                }
            }
            JsonNode _leaderIdNode = _node.get("leaderId");
            if (_leaderIdNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'leaderId', which is mandatory in version " + _version);
            } else {
                _object.leaderId = MessageUtil.jsonNodeToInt(_leaderIdNode, "PartitionData");
            }
            JsonNode _leaderEpochNode = _node.get("leaderEpoch");
            if (_leaderEpochNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'leaderEpoch', which is mandatory in version " + _version);
            } else {
                _object.leaderEpoch = MessageUtil.jsonNodeToInt(_leaderEpochNode, "PartitionData");
            }
            JsonNode _highWatermarkNode = _node.get("highWatermark");
            if (_highWatermarkNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'highWatermark', which is mandatory in version " + _version);
            } else {
                _object.highWatermark = MessageUtil.jsonNodeToLong(_highWatermarkNode, "PartitionData");
            }
            JsonNode _currentVotersNode = _node.get("currentVoters");
            if (_currentVotersNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'currentVoters', which is mandatory in version " + _version);
            } else {
                if (!_currentVotersNode.isArray()) {
                    throw new RuntimeException("PartitionData expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<ReplicaState> _collection = new ArrayList<ReplicaState>(_currentVotersNode.size());
                _object.currentVoters = _collection;
                for (JsonNode _element : _currentVotersNode) {
                    _collection.add(ReplicaStateJsonConverter.read(_element, _version));
                }
            }
            JsonNode _observersNode = _node.get("observers");
            if (_observersNode == null) {
                throw new RuntimeException("PartitionData: unable to locate field 'observers', which is mandatory in version " + _version);
            } else {
                if (!_observersNode.isArray()) {
                    throw new RuntimeException("PartitionData expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<ReplicaState> _collection = new ArrayList<ReplicaState>(_observersNode.size());
                _object.observers = _collection;
                for (JsonNode _element : _observersNode) {
                    _collection.add(ReplicaStateJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(PartitionData _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            _node.set("errorCode", new ShortNode(_object.errorCode));
            if (_version >= 2) {
                if (_object.errorMessage == null) {
                    _node.set("errorMessage", NullNode.instance);
                } else {
                    _node.set("errorMessage", new TextNode(_object.errorMessage));
                }
            }
            _node.set("leaderId", new IntNode(_object.leaderId));
            _node.set("leaderEpoch", new IntNode(_object.leaderEpoch));
            _node.set("highWatermark", new LongNode(_object.highWatermark));
            ArrayNode _currentVotersArray = new ArrayNode(JsonNodeFactory.instance);
            for (ReplicaState _element : _object.currentVoters) {
                _currentVotersArray.add(ReplicaStateJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("currentVoters", _currentVotersArray);
            ArrayNode _observersArray = new ArrayNode(JsonNodeFactory.instance);
            for (ReplicaState _element : _object.observers) {
                _observersArray.add(ReplicaStateJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("observers", _observersArray);
            return _node;
        }
        public static JsonNode write(PartitionData _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class ReplicaStateJsonConverter {
        public static ReplicaState read(JsonNode _node, short _version) {
            ReplicaState _object = new ReplicaState();
            JsonNode _replicaIdNode = _node.get("replicaId");
            if (_replicaIdNode == null) {
                throw new RuntimeException("ReplicaState: unable to locate field 'replicaId', which is mandatory in version " + _version);
            } else {
                _object.replicaId = MessageUtil.jsonNodeToInt(_replicaIdNode, "ReplicaState");
            }
            JsonNode _replicaDirectoryIdNode = _node.get("replicaDirectoryId");
            if (_replicaDirectoryIdNode == null) {
                if (_version >= 2) {
                    throw new RuntimeException("ReplicaState: unable to locate field 'replicaDirectoryId', which is mandatory in version " + _version);
                } else {
                    _object.replicaDirectoryId = Uuid.ZERO_UUID;
                }
            } else {
                if (!_replicaDirectoryIdNode.isTextual()) {
                    throw new RuntimeException("ReplicaState expected a JSON string type, but got " + _node.getNodeType());
                }
                _object.replicaDirectoryId = Uuid.fromString(_replicaDirectoryIdNode.asText());
            }
            JsonNode _logEndOffsetNode = _node.get("logEndOffset");
            if (_logEndOffsetNode == null) {
                throw new RuntimeException("ReplicaState: unable to locate field 'logEndOffset', which is mandatory in version " + _version);
            } else {
                _object.logEndOffset = MessageUtil.jsonNodeToLong(_logEndOffsetNode, "ReplicaState");
            }
            JsonNode _lastFetchTimestampNode = _node.get("lastFetchTimestamp");
            if (_lastFetchTimestampNode == null) {
                if (_version >= 1) {
                    throw new RuntimeException("ReplicaState: unable to locate field 'lastFetchTimestamp', which is mandatory in version " + _version);
                } else {
                    _object.lastFetchTimestamp = -1L;
                }
            } else {
                _object.lastFetchTimestamp = MessageUtil.jsonNodeToLong(_lastFetchTimestampNode, "ReplicaState");
            }
            JsonNode _lastCaughtUpTimestampNode = _node.get("lastCaughtUpTimestamp");
            if (_lastCaughtUpTimestampNode == null) {
                if (_version >= 1) {
                    throw new RuntimeException("ReplicaState: unable to locate field 'lastCaughtUpTimestamp', which is mandatory in version " + _version);
                } else {
                    _object.lastCaughtUpTimestamp = -1L;
                }
            } else {
                _object.lastCaughtUpTimestamp = MessageUtil.jsonNodeToLong(_lastCaughtUpTimestampNode, "ReplicaState");
            }
            return _object;
        }
        public static JsonNode write(ReplicaState _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("replicaId", new IntNode(_object.replicaId));
            if (_version >= 2) {
                _node.set("replicaDirectoryId", new TextNode(_object.replicaDirectoryId.toString()));
            } else {
                if (!_object.replicaDirectoryId.equals(Uuid.ZERO_UUID)) {
                    throw new UnsupportedVersionException("Attempted to write a non-default replicaDirectoryId at version " + _version);
                }
            }
            _node.set("logEndOffset", new LongNode(_object.logEndOffset));
            if (_version >= 1) {
                _node.set("lastFetchTimestamp", new LongNode(_object.lastFetchTimestamp));
            }
            if (_version >= 1) {
                _node.set("lastCaughtUpTimestamp", new LongNode(_object.lastCaughtUpTimestamp));
            }
            return _node;
        }
        public static JsonNode write(ReplicaState _object, short _version) {
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
