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
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.ShareGroupHeartbeatResponseData.*;

public class ShareGroupHeartbeatResponseDataJsonConverter {
    public static ShareGroupHeartbeatResponseData read(JsonNode _node, short _version) {
        ShareGroupHeartbeatResponseData _object = new ShareGroupHeartbeatResponseData();
        JsonNode _throttleTimeMsNode = _node.get("throttleTimeMs");
        if (_throttleTimeMsNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatResponseData: unable to locate field 'throttleTimeMs', which is mandatory in version " + _version);
        } else {
            _object.throttleTimeMs = MessageUtil.jsonNodeToInt(_throttleTimeMsNode, "ShareGroupHeartbeatResponseData");
        }
        JsonNode _errorCodeNode = _node.get("errorCode");
        if (_errorCodeNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatResponseData: unable to locate field 'errorCode', which is mandatory in version " + _version);
        } else {
            _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "ShareGroupHeartbeatResponseData");
        }
        JsonNode _errorMessageNode = _node.get("errorMessage");
        if (_errorMessageNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatResponseData: unable to locate field 'errorMessage', which is mandatory in version " + _version);
        } else {
            if (_errorMessageNode.isNull()) {
                _object.errorMessage = null;
            } else {
                if (!_errorMessageNode.isTextual()) {
                    throw new RuntimeException("ShareGroupHeartbeatResponseData expected a string type, but got " + _node.getNodeType());
                }
                _object.errorMessage = _errorMessageNode.asText();
            }
        }
        JsonNode _memberIdNode = _node.get("memberId");
        if (_memberIdNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatResponseData: unable to locate field 'memberId', which is mandatory in version " + _version);
        } else {
            if (_memberIdNode.isNull()) {
                _object.memberId = null;
            } else {
                if (!_memberIdNode.isTextual()) {
                    throw new RuntimeException("ShareGroupHeartbeatResponseData expected a string type, but got " + _node.getNodeType());
                }
                _object.memberId = _memberIdNode.asText();
            }
        }
        JsonNode _memberEpochNode = _node.get("memberEpoch");
        if (_memberEpochNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatResponseData: unable to locate field 'memberEpoch', which is mandatory in version " + _version);
        } else {
            _object.memberEpoch = MessageUtil.jsonNodeToInt(_memberEpochNode, "ShareGroupHeartbeatResponseData");
        }
        JsonNode _heartbeatIntervalMsNode = _node.get("heartbeatIntervalMs");
        if (_heartbeatIntervalMsNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatResponseData: unable to locate field 'heartbeatIntervalMs', which is mandatory in version " + _version);
        } else {
            _object.heartbeatIntervalMs = MessageUtil.jsonNodeToInt(_heartbeatIntervalMsNode, "ShareGroupHeartbeatResponseData");
        }
        JsonNode _assignmentNode = _node.get("assignment");
        if (_assignmentNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatResponseData: unable to locate field 'assignment', which is mandatory in version " + _version);
        } else {
            if (_assignmentNode.isNull()) {
                _object.assignment = null;
            } else {
                _object.assignment = AssignmentJsonConverter.read(_assignmentNode, _version);
            }
        }
        return _object;
    }
    public static JsonNode write(ShareGroupHeartbeatResponseData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("throttleTimeMs", new IntNode(_object.throttleTimeMs));
        _node.set("errorCode", new ShortNode(_object.errorCode));
        if (_object.errorMessage == null) {
            _node.set("errorMessage", NullNode.instance);
        } else {
            _node.set("errorMessage", new TextNode(_object.errorMessage));
        }
        if (_object.memberId == null) {
            _node.set("memberId", NullNode.instance);
        } else {
            _node.set("memberId", new TextNode(_object.memberId));
        }
        _node.set("memberEpoch", new IntNode(_object.memberEpoch));
        _node.set("heartbeatIntervalMs", new IntNode(_object.heartbeatIntervalMs));
        if (_object.assignment == null) {
            _node.set("assignment", NullNode.instance);
        } else {
            _node.set("assignment", AssignmentJsonConverter.write(_object.assignment, _version, _serializeRecords));
        }
        return _node;
    }
    public static JsonNode write(ShareGroupHeartbeatResponseData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class AssignmentJsonConverter {
        public static Assignment read(JsonNode _node, short _version) {
            Assignment _object = new Assignment();
            JsonNode _topicPartitionsNode = _node.get("topicPartitions");
            if (_topicPartitionsNode == null) {
                throw new RuntimeException("Assignment: unable to locate field 'topicPartitions', which is mandatory in version " + _version);
            } else {
                if (!_topicPartitionsNode.isArray()) {
                    throw new RuntimeException("Assignment expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<TopicPartitions> _collection = new ArrayList<TopicPartitions>(_topicPartitionsNode.size());
                _object.topicPartitions = _collection;
                for (JsonNode _element : _topicPartitionsNode) {
                    _collection.add(TopicPartitionsJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(Assignment _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            ArrayNode _topicPartitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (TopicPartitions _element : _object.topicPartitions) {
                _topicPartitionsArray.add(TopicPartitionsJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("topicPartitions", _topicPartitionsArray);
            return _node;
        }
        public static JsonNode write(Assignment _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class TopicPartitionsJsonConverter {
        public static TopicPartitions read(JsonNode _node, short _version) {
            TopicPartitions _object = new TopicPartitions();
            JsonNode _topicIdNode = _node.get("topicId");
            if (_topicIdNode == null) {
                throw new RuntimeException("TopicPartitions: unable to locate field 'topicId', which is mandatory in version " + _version);
            } else {
                if (!_topicIdNode.isTextual()) {
                    throw new RuntimeException("TopicPartitions expected a JSON string type, but got " + _node.getNodeType());
                }
                _object.topicId = Uuid.fromString(_topicIdNode.asText());
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("TopicPartitions: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("TopicPartitions expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<Integer> _collection = new ArrayList<Integer>(_partitionsNode.size());
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(MessageUtil.jsonNodeToInt(_element, "TopicPartitions element"));
                }
            }
            return _object;
        }
        public static JsonNode write(TopicPartitions _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("topicId", new TextNode(_object.topicId.toString()));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (Integer _element : _object.partitions) {
                _partitionsArray.add(new IntNode(_element));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
        public static JsonNode write(TopicPartitions _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
