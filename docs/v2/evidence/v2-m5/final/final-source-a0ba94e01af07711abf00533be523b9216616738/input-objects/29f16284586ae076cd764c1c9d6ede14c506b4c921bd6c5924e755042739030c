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
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.RemoveRaftVoterRequestData.*;

public class RemoveRaftVoterRequestDataJsonConverter {
    public static RemoveRaftVoterRequestData read(JsonNode _node, short _version) {
        RemoveRaftVoterRequestData _object = new RemoveRaftVoterRequestData();
        JsonNode _clusterIdNode = _node.get("clusterId");
        if (_clusterIdNode == null) {
            throw new RuntimeException("RemoveRaftVoterRequestData: unable to locate field 'clusterId', which is mandatory in version " + _version);
        } else {
            if (_clusterIdNode.isNull()) {
                _object.clusterId = null;
            } else {
                if (!_clusterIdNode.isTextual()) {
                    throw new RuntimeException("RemoveRaftVoterRequestData expected a string type, but got " + _node.getNodeType());
                }
                _object.clusterId = _clusterIdNode.asText();
            }
        }
        JsonNode _voterIdNode = _node.get("voterId");
        if (_voterIdNode == null) {
            throw new RuntimeException("RemoveRaftVoterRequestData: unable to locate field 'voterId', which is mandatory in version " + _version);
        } else {
            _object.voterId = MessageUtil.jsonNodeToInt(_voterIdNode, "RemoveRaftVoterRequestData");
        }
        JsonNode _voterDirectoryIdNode = _node.get("voterDirectoryId");
        if (_voterDirectoryIdNode == null) {
            throw new RuntimeException("RemoveRaftVoterRequestData: unable to locate field 'voterDirectoryId', which is mandatory in version " + _version);
        } else {
            if (!_voterDirectoryIdNode.isTextual()) {
                throw new RuntimeException("RemoveRaftVoterRequestData expected a JSON string type, but got " + _node.getNodeType());
            }
            _object.voterDirectoryId = Uuid.fromString(_voterDirectoryIdNode.asText());
        }
        return _object;
    }
    public static JsonNode write(RemoveRaftVoterRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        if (_object.clusterId == null) {
            _node.set("clusterId", NullNode.instance);
        } else {
            _node.set("clusterId", new TextNode(_object.clusterId));
        }
        _node.set("voterId", new IntNode(_object.voterId));
        _node.set("voterDirectoryId", new TextNode(_object.voterDirectoryId.toString()));
        return _node;
    }
    public static JsonNode write(RemoveRaftVoterRequestData _object, short _version) {
        return write(_object, _version, true);
    }
}
