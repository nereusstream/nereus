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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.VotersRecord.*;

public class VotersRecordJsonConverter {
    public static VotersRecord read(JsonNode _node, short _version) {
        VotersRecord _object = new VotersRecord();
        JsonNode _versionNode = _node.get("version");
        if (_versionNode == null) {
            throw new RuntimeException("VotersRecord: unable to locate field 'version', which is mandatory in version " + _version);
        } else {
            _object.version = MessageUtil.jsonNodeToShort(_versionNode, "VotersRecord");
        }
        JsonNode _votersNode = _node.get("voters");
        if (_votersNode == null) {
            throw new RuntimeException("VotersRecord: unable to locate field 'voters', which is mandatory in version " + _version);
        } else {
            if (!_votersNode.isArray()) {
                throw new RuntimeException("VotersRecord expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<Voter> _collection = new ArrayList<Voter>(_votersNode.size());
            _object.voters = _collection;
            for (JsonNode _element : _votersNode) {
                _collection.add(VoterJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(VotersRecord _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("version", new ShortNode(_object.version));
        ArrayNode _votersArray = new ArrayNode(JsonNodeFactory.instance);
        for (Voter _element : _object.voters) {
            _votersArray.add(VoterJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("voters", _votersArray);
        return _node;
    }
    public static JsonNode write(VotersRecord _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class EndpointJsonConverter {
        public static Endpoint read(JsonNode _node, short _version) {
            Endpoint _object = new Endpoint();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("Endpoint: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("Endpoint expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _hostNode = _node.get("host");
            if (_hostNode == null) {
                throw new RuntimeException("Endpoint: unable to locate field 'host', which is mandatory in version " + _version);
            } else {
                if (!_hostNode.isTextual()) {
                    throw new RuntimeException("Endpoint expected a string type, but got " + _node.getNodeType());
                }
                _object.host = _hostNode.asText();
            }
            JsonNode _portNode = _node.get("port");
            if (_portNode == null) {
                throw new RuntimeException("Endpoint: unable to locate field 'port', which is mandatory in version " + _version);
            } else {
                _object.port = MessageUtil.jsonNodeToUnsignedShort(_portNode, "Endpoint");
            }
            return _object;
        }
        public static JsonNode write(Endpoint _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            _node.set("host", new TextNode(_object.host));
            _node.set("port", new IntNode(_object.port));
            return _node;
        }
        public static JsonNode write(Endpoint _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class KRaftVersionFeatureJsonConverter {
        public static KRaftVersionFeature read(JsonNode _node, short _version) {
            KRaftVersionFeature _object = new KRaftVersionFeature();
            JsonNode _minSupportedVersionNode = _node.get("minSupportedVersion");
            if (_minSupportedVersionNode == null) {
                throw new RuntimeException("KRaftVersionFeature: unable to locate field 'minSupportedVersion', which is mandatory in version " + _version);
            } else {
                _object.minSupportedVersion = MessageUtil.jsonNodeToShort(_minSupportedVersionNode, "KRaftVersionFeature");
            }
            JsonNode _maxSupportedVersionNode = _node.get("maxSupportedVersion");
            if (_maxSupportedVersionNode == null) {
                throw new RuntimeException("KRaftVersionFeature: unable to locate field 'maxSupportedVersion', which is mandatory in version " + _version);
            } else {
                _object.maxSupportedVersion = MessageUtil.jsonNodeToShort(_maxSupportedVersionNode, "KRaftVersionFeature");
            }
            return _object;
        }
        public static JsonNode write(KRaftVersionFeature _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("minSupportedVersion", new ShortNode(_object.minSupportedVersion));
            _node.set("maxSupportedVersion", new ShortNode(_object.maxSupportedVersion));
            return _node;
        }
        public static JsonNode write(KRaftVersionFeature _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class VoterJsonConverter {
        public static Voter read(JsonNode _node, short _version) {
            Voter _object = new Voter();
            JsonNode _voterIdNode = _node.get("voterId");
            if (_voterIdNode == null) {
                throw new RuntimeException("Voter: unable to locate field 'voterId', which is mandatory in version " + _version);
            } else {
                _object.voterId = MessageUtil.jsonNodeToInt(_voterIdNode, "Voter");
            }
            JsonNode _voterDirectoryIdNode = _node.get("voterDirectoryId");
            if (_voterDirectoryIdNode == null) {
                throw new RuntimeException("Voter: unable to locate field 'voterDirectoryId', which is mandatory in version " + _version);
            } else {
                if (!_voterDirectoryIdNode.isTextual()) {
                    throw new RuntimeException("Voter expected a JSON string type, but got " + _node.getNodeType());
                }
                _object.voterDirectoryId = Uuid.fromString(_voterDirectoryIdNode.asText());
            }
            JsonNode _endpointsNode = _node.get("endpoints");
            if (_endpointsNode == null) {
                throw new RuntimeException("Voter: unable to locate field 'endpoints', which is mandatory in version " + _version);
            } else {
                if (!_endpointsNode.isArray()) {
                    throw new RuntimeException("Voter expected a JSON array, but got " + _node.getNodeType());
                }
                EndpointCollection _collection = new EndpointCollection(_endpointsNode.size());
                _object.endpoints = _collection;
                for (JsonNode _element : _endpointsNode) {
                    _collection.add(EndpointJsonConverter.read(_element, _version));
                }
            }
            JsonNode _kRaftVersionFeatureNode = _node.get("kRaftVersionFeature");
            if (_kRaftVersionFeatureNode == null) {
                throw new RuntimeException("Voter: unable to locate field 'kRaftVersionFeature', which is mandatory in version " + _version);
            } else {
                _object.kRaftVersionFeature = KRaftVersionFeatureJsonConverter.read(_kRaftVersionFeatureNode, _version);
            }
            return _object;
        }
        public static JsonNode write(Voter _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("voterId", new IntNode(_object.voterId));
            _node.set("voterDirectoryId", new TextNode(_object.voterDirectoryId.toString()));
            ArrayNode _endpointsArray = new ArrayNode(JsonNodeFactory.instance);
            for (Endpoint _element : _object.endpoints) {
                _endpointsArray.add(EndpointJsonConverter.write(_element, _version, _serializeRecords));
            }
            _node.set("endpoints", _endpointsArray);
            _node.set("kRaftVersionFeature", KRaftVersionFeatureJsonConverter.write(_object.kRaftVersionFeature, _version, _serializeRecords));
            return _node;
        }
        public static JsonNode write(Voter _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
