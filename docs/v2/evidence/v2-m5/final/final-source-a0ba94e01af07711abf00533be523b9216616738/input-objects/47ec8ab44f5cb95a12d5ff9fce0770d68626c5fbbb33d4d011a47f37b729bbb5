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
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.ShareGroupHeartbeatRequestData.*;

public class ShareGroupHeartbeatRequestDataJsonConverter {
    public static ShareGroupHeartbeatRequestData read(JsonNode _node, short _version) {
        ShareGroupHeartbeatRequestData _object = new ShareGroupHeartbeatRequestData();
        JsonNode _groupIdNode = _node.get("groupId");
        if (_groupIdNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatRequestData: unable to locate field 'groupId', which is mandatory in version " + _version);
        } else {
            if (!_groupIdNode.isTextual()) {
                throw new RuntimeException("ShareGroupHeartbeatRequestData expected a string type, but got " + _node.getNodeType());
            }
            _object.groupId = _groupIdNode.asText();
        }
        JsonNode _memberIdNode = _node.get("memberId");
        if (_memberIdNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatRequestData: unable to locate field 'memberId', which is mandatory in version " + _version);
        } else {
            if (!_memberIdNode.isTextual()) {
                throw new RuntimeException("ShareGroupHeartbeatRequestData expected a string type, but got " + _node.getNodeType());
            }
            _object.memberId = _memberIdNode.asText();
        }
        JsonNode _memberEpochNode = _node.get("memberEpoch");
        if (_memberEpochNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatRequestData: unable to locate field 'memberEpoch', which is mandatory in version " + _version);
        } else {
            _object.memberEpoch = MessageUtil.jsonNodeToInt(_memberEpochNode, "ShareGroupHeartbeatRequestData");
        }
        JsonNode _rackIdNode = _node.get("rackId");
        if (_rackIdNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatRequestData: unable to locate field 'rackId', which is mandatory in version " + _version);
        } else {
            if (_rackIdNode.isNull()) {
                _object.rackId = null;
            } else {
                if (!_rackIdNode.isTextual()) {
                    throw new RuntimeException("ShareGroupHeartbeatRequestData expected a string type, but got " + _node.getNodeType());
                }
                _object.rackId = _rackIdNode.asText();
            }
        }
        JsonNode _subscribedTopicNamesNode = _node.get("subscribedTopicNames");
        if (_subscribedTopicNamesNode == null) {
            throw new RuntimeException("ShareGroupHeartbeatRequestData: unable to locate field 'subscribedTopicNames', which is mandatory in version " + _version);
        } else {
            if (_subscribedTopicNamesNode.isNull()) {
                _object.subscribedTopicNames = null;
            } else {
                if (!_subscribedTopicNamesNode.isArray()) {
                    throw new RuntimeException("ShareGroupHeartbeatRequestData expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<String> _collection = new ArrayList<String>(_subscribedTopicNamesNode.size());
                _object.subscribedTopicNames = _collection;
                for (JsonNode _element : _subscribedTopicNamesNode) {
                    if (!_element.isTextual()) {
                        throw new RuntimeException("ShareGroupHeartbeatRequestData element expected a string type, but got " + _node.getNodeType());
                    }
                    _collection.add(_element.asText());
                }
            }
        }
        return _object;
    }
    public static JsonNode write(ShareGroupHeartbeatRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("groupId", new TextNode(_object.groupId));
        _node.set("memberId", new TextNode(_object.memberId));
        _node.set("memberEpoch", new IntNode(_object.memberEpoch));
        if (_object.rackId == null) {
            _node.set("rackId", NullNode.instance);
        } else {
            _node.set("rackId", new TextNode(_object.rackId));
        }
        if (_object.subscribedTopicNames == null) {
            _node.set("subscribedTopicNames", NullNode.instance);
        } else {
            ArrayNode _subscribedTopicNamesArray = new ArrayNode(JsonNodeFactory.instance);
            for (String _element : _object.subscribedTopicNames) {
                _subscribedTopicNamesArray.add(new TextNode(_element));
            }
            _node.set("subscribedTopicNames", _subscribedTopicNamesArray);
        }
        return _node;
    }
    public static JsonNode write(ShareGroupHeartbeatRequestData _object, short _version) {
        return write(_object, _version, true);
    }
}
