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
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.UpdateRaftVoterResponseData.*;

public class UpdateRaftVoterResponseDataJsonConverter {
    public static UpdateRaftVoterResponseData read(JsonNode _node, short _version) {
        UpdateRaftVoterResponseData _object = new UpdateRaftVoterResponseData();
        JsonNode _throttleTimeMsNode = _node.get("throttleTimeMs");
        if (_throttleTimeMsNode == null) {
            throw new RuntimeException("UpdateRaftVoterResponseData: unable to locate field 'throttleTimeMs', which is mandatory in version " + _version);
        } else {
            _object.throttleTimeMs = MessageUtil.jsonNodeToInt(_throttleTimeMsNode, "UpdateRaftVoterResponseData");
        }
        JsonNode _errorCodeNode = _node.get("errorCode");
        if (_errorCodeNode == null) {
            throw new RuntimeException("UpdateRaftVoterResponseData: unable to locate field 'errorCode', which is mandatory in version " + _version);
        } else {
            _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "UpdateRaftVoterResponseData");
        }
        JsonNode _currentLeaderNode = _node.get("currentLeader");
        if (_currentLeaderNode == null) {
            _object.currentLeader = new CurrentLeader();
        } else {
            _object.currentLeader = CurrentLeaderJsonConverter.read(_currentLeaderNode, _version);
        }
        return _object;
    }
    public static JsonNode write(UpdateRaftVoterResponseData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("throttleTimeMs", new IntNode(_object.throttleTimeMs));
        _node.set("errorCode", new ShortNode(_object.errorCode));
        if (!_object.currentLeader.equals(new CurrentLeader())) {
            _node.set("currentLeader", CurrentLeaderJsonConverter.write(_object.currentLeader, _version, _serializeRecords));
        }
        return _node;
    }
    public static JsonNode write(UpdateRaftVoterResponseData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class CurrentLeaderJsonConverter {
        public static CurrentLeader read(JsonNode _node, short _version) {
            CurrentLeader _object = new CurrentLeader();
            JsonNode _leaderIdNode = _node.get("leaderId");
            if (_leaderIdNode == null) {
                throw new RuntimeException("CurrentLeader: unable to locate field 'leaderId', which is mandatory in version " + _version);
            } else {
                _object.leaderId = MessageUtil.jsonNodeToInt(_leaderIdNode, "CurrentLeader");
            }
            JsonNode _leaderEpochNode = _node.get("leaderEpoch");
            if (_leaderEpochNode == null) {
                throw new RuntimeException("CurrentLeader: unable to locate field 'leaderEpoch', which is mandatory in version " + _version);
            } else {
                _object.leaderEpoch = MessageUtil.jsonNodeToInt(_leaderEpochNode, "CurrentLeader");
            }
            JsonNode _hostNode = _node.get("host");
            if (_hostNode == null) {
                throw new RuntimeException("CurrentLeader: unable to locate field 'host', which is mandatory in version " + _version);
            } else {
                if (!_hostNode.isTextual()) {
                    throw new RuntimeException("CurrentLeader expected a string type, but got " + _node.getNodeType());
                }
                _object.host = _hostNode.asText();
            }
            JsonNode _portNode = _node.get("port");
            if (_portNode == null) {
                throw new RuntimeException("CurrentLeader: unable to locate field 'port', which is mandatory in version " + _version);
            } else {
                _object.port = MessageUtil.jsonNodeToInt(_portNode, "CurrentLeader");
            }
            return _object;
        }
        public static JsonNode write(CurrentLeader _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("leaderId", new IntNode(_object.leaderId));
            _node.set("leaderEpoch", new IntNode(_object.leaderEpoch));
            _node.set("host", new TextNode(_object.host));
            _node.set("port", new IntNode(_object.port));
            return _node;
        }
        public static JsonNode write(CurrentLeader _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
