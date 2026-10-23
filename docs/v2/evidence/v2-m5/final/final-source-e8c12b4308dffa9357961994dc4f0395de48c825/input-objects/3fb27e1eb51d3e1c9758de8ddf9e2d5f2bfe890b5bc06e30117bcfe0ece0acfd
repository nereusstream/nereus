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

import static org.apache.kafka.common.message.ShareGroupDescribeResponseData.*;

public class ShareGroupDescribeResponseDataJsonConverter {
    public static ShareGroupDescribeResponseData read(JsonNode _node, short _version) {
        ShareGroupDescribeResponseData _object = new ShareGroupDescribeResponseData();
        JsonNode _throttleTimeMsNode = _node.get("throttleTimeMs");
        if (_throttleTimeMsNode == null) {
            throw new RuntimeException("ShareGroupDescribeResponseData: unable to locate field 'throttleTimeMs', which is mandatory in version " + _version);
        } else {
            _object.throttleTimeMs = MessageUtil.jsonNodeToInt(_throttleTimeMsNode, "ShareGroupDescribeResponseData");
        }
        JsonNode _groupsNode = _node.get("groups");
        if (_groupsNode == null) {
            throw new RuntimeException("ShareGroupDescribeResponseData: unable to locate field 'groups', which is mandatory in version " + _version);
        } else {
            if (!_groupsNode.isArray()) {
                throw new RuntimeException("ShareGroupDescribeResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<DescribedGroup> _collection = new ArrayList<DescribedGroup>(_groupsNode.size());
            _object.groups = _collection;
            for (JsonNode _element : _groupsNode) {
                _collection.add(DescribedGroupJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(ShareGroupDescribeResponseData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("throttleTimeMs", new IntNode(_object.throttleTimeMs));
        ArrayNode _groupsArray = new ArrayNode(JsonNodeFactory.instance);
        for (DescribedGroup _element : _object.groups) {
            _groupsArray.add(DescribedGroupJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("groups", _groupsArray);
        return _node;
    }
    public static JsonNode write(ShareGroupDescribeResponseData _object, short _version) {
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
    
    public static class DescribedGroupJsonConverter {
        public static DescribedGroup read(JsonNode _node, short _version) {
            DescribedGroup _object = new DescribedGroup();
            JsonNode _errorCodeNode = _node.get("errorCode");
            if (_errorCodeNode == null) {
                throw new RuntimeException("DescribedGroup: unable to locate field 'errorCode', which is mandatory in version " + _version);
            } else {
                _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "DescribedGroup");
            }
            JsonNode _errorMessageNode = _node.get("errorMessage");
            if (_errorMessageNode == null) {
                throw new RuntimeException("DescribedGroup: unable to locate field 'errorMessage', which is mandatory in version " + _version);
            } else {
                if (_errorMessageNode.isNull()) {
                    _object.errorMessage = null;
                } else {
                    if (!_errorMessageNode.isTextual()) {
                        throw new RuntimeException("DescribedGroup expected a string type, but got " + _node.getNodeType());
                    }
                    _object.errorMessage = _errorMessageNode.asText();
                }
            }
            JsonNode _groupIdNode = _node.get("groupId");
            if (_groupIdNode == null) {
                throw new RuntimeException("DescribedGroup: unable to locate field 'groupId', which is mandatory in version " + _version);
            } else {
                if (!_groupIdNode.isTextual()) {
                    throw new RuntimeException("DescribedGroup expected a string type, but got " + _node.getNodeType());
                }
                _object.groupId = _groupIdNode.asText();
            }
            JsonNode _groupStateNode = _node.get("groupState");
            if (_groupStateNode == null) {
                throw new RuntimeException("DescribedGroup: unable to locate field 'groupState', which is mandatory in version " + _version);
            } else {
                if (!_groupStateNode.isTextual()) {
                    throw new RuntimeException("DescribedGroup expected a string type, but got " + _node.getNodeType());
                }
                _object.groupState = _groupStateNode.asText();
            }
            JsonNode _groupEpochNode = _node.get("groupEpoch");
            if (_groupEpochNode == null) {
                throw new RuntimeException("DescribedGroup: unable to locate field 'groupEpoch', which is mandatory in version " + _version);
            } else {
                _object.groupEpoch = MessageUtil.jsonNodeToInt(_groupEpochNode, "DescribedGroup");
            }
            JsonNode _assignmentEpochNode = _node.get("assignmentEpoch");
            if (_assignmentEpochNode == null) {
                throw new RuntimeException("DescribedGroup: unable to locate field 'assignmentEpoch', which is mandatory in version " + _version);
            } else {
                _object.assignmentEpoch = MessageUtil.jsonNodeToInt(_assignmentEpochNode, "DescribedGroup");
            }
            JsonNode _assignorNameNode = _node.get("assignorName");
            if (_assignorNameNode == null) {
                throw new RuntimeException("DescribedGroup: unable to locate field 'assignorName', which is mandatory in version " + _version);
            } else {
                if (!_assignorNameNode.isTextual()) {
                    throw new RuntimeException("DescribedGroup expected a string type, but got " + _node.getNodeType());
                }
                _object.assignorName = _assignorNameNode.asText();
            }
            JsonNode _membersNode = _node.get("members");
            if (_membersNode == null) {
                throw new RuntimeException("DescribedGroup: unable to locate field 'members', which is mandatory in version " + _version);
            } else {
                if (!_membersNode.isArray()) {
                    throw new RuntimeException("DescribedGroup expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<Member> _collection = new ArrayList<Member>(_membersNode.size());
                _object.members = _collection;
                for (JsonNode _element : _membersNode) {
                    _collection.add(MemberJsonConverter.read(_element, _version));
                }
            }
            JsonNode _authorizedOperationsNode = _node.get("authorizedOperations");
            if (_authorizedOperationsNode == null) {
                throw new RuntimeException("DescribedGroup: unable to locate field 'authorizedOperations', which is mandatory in version " + _version);
            } else {
                _object.authorizedOperations = MessageUtil.jsonNodeToInt(_authorizedOperationsNode, "DescribedGroup");
            }
            return _object;
        }
        public static JsonNode write(DescribedGroup _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("errorCode", new ShortNode(_object.errorCode));
            if (_object.errorMessage == null) {
                _node.set("errorMessage", NullNode.instance);
            } else {
                _node.set("errorMessage", new TextNode(_object.errorMessage));
            }
            _node.set("groupId", new TextNode(_object.groupId));
            _node.set("groupState", new TextNode(_object.groupState));
            _node.set("groupEpoch", new IntNode(_object.groupEpoch));
            _node.set("assignmentEpoch", new IntNode(_object.assignmentEpoch));
            _node.set("assignorName", new TextNode(_object.assignorName));
            ArrayNode _membersArray = new ArrayNode(JsonNodeFactory.instance);
            for (Member _element : _object.members) {
                _membersArray.add(MemberJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("members", _membersArray);
            _node.set("authorizedOperations", new IntNode(_object.authorizedOperations));
            return _node;
        }
        public static JsonNode write(DescribedGroup _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class MemberJsonConverter {
        public static Member read(JsonNode _node, short _version) {
            Member _object = new Member();
            JsonNode _memberIdNode = _node.get("memberId");
            if (_memberIdNode == null) {
                throw new RuntimeException("Member: unable to locate field 'memberId', which is mandatory in version " + _version);
            } else {
                if (!_memberIdNode.isTextual()) {
                    throw new RuntimeException("Member expected a string type, but got " + _node.getNodeType());
                }
                _object.memberId = _memberIdNode.asText();
            }
            JsonNode _rackIdNode = _node.get("rackId");
            if (_rackIdNode == null) {
                throw new RuntimeException("Member: unable to locate field 'rackId', which is mandatory in version " + _version);
            } else {
                if (_rackIdNode.isNull()) {
                    _object.rackId = null;
                } else {
                    if (!_rackIdNode.isTextual()) {
                        throw new RuntimeException("Member expected a string type, but got " + _node.getNodeType());
                    }
                    _object.rackId = _rackIdNode.asText();
                }
            }
            JsonNode _memberEpochNode = _node.get("memberEpoch");
            if (_memberEpochNode == null) {
                throw new RuntimeException("Member: unable to locate field 'memberEpoch', which is mandatory in version " + _version);
            } else {
                _object.memberEpoch = MessageUtil.jsonNodeToInt(_memberEpochNode, "Member");
            }
            JsonNode _clientIdNode = _node.get("clientId");
            if (_clientIdNode == null) {
                throw new RuntimeException("Member: unable to locate field 'clientId', which is mandatory in version " + _version);
            } else {
                if (!_clientIdNode.isTextual()) {
                    throw new RuntimeException("Member expected a string type, but got " + _node.getNodeType());
                }
                _object.clientId = _clientIdNode.asText();
            }
            JsonNode _clientHostNode = _node.get("clientHost");
            if (_clientHostNode == null) {
                throw new RuntimeException("Member: unable to locate field 'clientHost', which is mandatory in version " + _version);
            } else {
                if (!_clientHostNode.isTextual()) {
                    throw new RuntimeException("Member expected a string type, but got " + _node.getNodeType());
                }
                _object.clientHost = _clientHostNode.asText();
            }
            JsonNode _subscribedTopicNamesNode = _node.get("subscribedTopicNames");
            if (_subscribedTopicNamesNode == null) {
                throw new RuntimeException("Member: unable to locate field 'subscribedTopicNames', which is mandatory in version " + _version);
            } else {
                if (!_subscribedTopicNamesNode.isArray()) {
                    throw new RuntimeException("Member expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<String> _collection = new ArrayList<String>(_subscribedTopicNamesNode.size());
                _object.subscribedTopicNames = _collection;
                for (JsonNode _element : _subscribedTopicNamesNode) {
                    if (!_element.isTextual()) {
                        throw new RuntimeException("Member element expected a string type, but got " + _node.getNodeType());
                    }
                    _collection.add(_element.asText());
                }
            }
            JsonNode _assignmentNode = _node.get("assignment");
            if (_assignmentNode == null) {
                throw new RuntimeException("Member: unable to locate field 'assignment', which is mandatory in version " + _version);
            } else {
                _object.assignment = AssignmentJsonConverter.read(_assignmentNode, _version);
            }
            return _object;
        }
        public static JsonNode write(Member _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("memberId", new TextNode(_object.memberId));
            if (_object.rackId == null) {
                _node.set("rackId", NullNode.instance);
            } else {
                _node.set("rackId", new TextNode(_object.rackId));
            }
            _node.set("memberEpoch", new IntNode(_object.memberEpoch));
            _node.set("clientId", new TextNode(_object.clientId));
            _node.set("clientHost", new TextNode(_object.clientHost));
            ArrayNode _subscribedTopicNamesArray = new ArrayNode(JsonNodeFactory.instance);
            for (String _element : _object.subscribedTopicNames) {
                _subscribedTopicNamesArray.add(new TextNode(_element));
            }
            _node.set("subscribedTopicNames", _subscribedTopicNamesArray);
            _node.set("assignment", AssignmentJsonConverter.write(_object.assignment, _version, _serializeRecords));
            return _node;
        }
        public static JsonNode write(Member _object, short _version) {
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
            JsonNode _topicNameNode = _node.get("topicName");
            if (_topicNameNode == null) {
                throw new RuntimeException("TopicPartitions: unable to locate field 'topicName', which is mandatory in version " + _version);
            } else {
                if (!_topicNameNode.isTextual()) {
                    throw new RuntimeException("TopicPartitions expected a string type, but got " + _node.getNodeType());
                }
                _object.topicName = _topicNameNode.asText();
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
            _node.set("topicName", new TextNode(_object.topicName));
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
