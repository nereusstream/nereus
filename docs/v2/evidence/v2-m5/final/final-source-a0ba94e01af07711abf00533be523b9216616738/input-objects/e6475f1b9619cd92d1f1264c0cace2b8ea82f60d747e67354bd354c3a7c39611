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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.metadata.RegisterControllerRecord.*;

public class RegisterControllerRecordJsonConverter {
    public static RegisterControllerRecord read(JsonNode _node, short _version) {
        RegisterControllerRecord _object = new RegisterControllerRecord();
        JsonNode _controllerIdNode = _node.get("controllerId");
        if (_controllerIdNode == null) {
            throw new RuntimeException("RegisterControllerRecord: unable to locate field 'controllerId', which is mandatory in version " + _version);
        } else {
            _object.controllerId = MessageUtil.jsonNodeToInt(_controllerIdNode, "RegisterControllerRecord");
        }
        JsonNode _incarnationIdNode = _node.get("incarnationId");
        if (_incarnationIdNode == null) {
            throw new RuntimeException("RegisterControllerRecord: unable to locate field 'incarnationId', which is mandatory in version " + _version);
        } else {
            if (!_incarnationIdNode.isTextual()) {
                throw new RuntimeException("RegisterControllerRecord expected a JSON string type, but got " + _node.getNodeType());
            }
            _object.incarnationId = Uuid.fromString(_incarnationIdNode.asText());
        }
        JsonNode _zkMigrationReadyNode = _node.get("zkMigrationReady");
        if (_zkMigrationReadyNode == null) {
            throw new RuntimeException("RegisterControllerRecord: unable to locate field 'zkMigrationReady', which is mandatory in version " + _version);
        } else {
            if (!_zkMigrationReadyNode.isBoolean()) {
                throw new RuntimeException("RegisterControllerRecord expected Boolean type, but got " + _node.getNodeType());
            }
            _object.zkMigrationReady = _zkMigrationReadyNode.asBoolean();
        }
        JsonNode _endPointsNode = _node.get("endPoints");
        if (_endPointsNode == null) {
            throw new RuntimeException("RegisterControllerRecord: unable to locate field 'endPoints', which is mandatory in version " + _version);
        } else {
            if (!_endPointsNode.isArray()) {
                throw new RuntimeException("RegisterControllerRecord expected a JSON array, but got " + _node.getNodeType());
            }
            ControllerEndpointCollection _collection = new ControllerEndpointCollection(_endPointsNode.size());
            _object.endPoints = _collection;
            for (JsonNode _element : _endPointsNode) {
                _collection.add(ControllerEndpointJsonConverter.read(_element, _version));
            }
        }
        JsonNode _featuresNode = _node.get("features");
        if (_featuresNode == null) {
            throw new RuntimeException("RegisterControllerRecord: unable to locate field 'features', which is mandatory in version " + _version);
        } else {
            if (!_featuresNode.isArray()) {
                throw new RuntimeException("RegisterControllerRecord expected a JSON array, but got " + _node.getNodeType());
            }
            ControllerFeatureCollection _collection = new ControllerFeatureCollection(_featuresNode.size());
            _object.features = _collection;
            for (JsonNode _element : _featuresNode) {
                _collection.add(ControllerFeatureJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(RegisterControllerRecord _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("controllerId", new IntNode(_object.controllerId));
        _node.set("incarnationId", new TextNode(_object.incarnationId.toString()));
        _node.set("zkMigrationReady", BooleanNode.valueOf(_object.zkMigrationReady));
        ArrayNode _endPointsArray = new ArrayNode(JsonNodeFactory.instance);
        for (ControllerEndpoint _element : _object.endPoints) {
            _endPointsArray.add(ControllerEndpointJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("endPoints", _endPointsArray);
        ArrayNode _featuresArray = new ArrayNode(JsonNodeFactory.instance);
        for (ControllerFeature _element : _object.features) {
            _featuresArray.add(ControllerFeatureJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("features", _featuresArray);
        return _node;
    }
    public static JsonNode write(RegisterControllerRecord _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class ControllerEndpointJsonConverter {
        public static ControllerEndpoint read(JsonNode _node, short _version) {
            ControllerEndpoint _object = new ControllerEndpoint();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("ControllerEndpoint: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("ControllerEndpoint expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _hostNode = _node.get("host");
            if (_hostNode == null) {
                throw new RuntimeException("ControllerEndpoint: unable to locate field 'host', which is mandatory in version " + _version);
            } else {
                if (!_hostNode.isTextual()) {
                    throw new RuntimeException("ControllerEndpoint expected a string type, but got " + _node.getNodeType());
                }
                _object.host = _hostNode.asText();
            }
            JsonNode _portNode = _node.get("port");
            if (_portNode == null) {
                throw new RuntimeException("ControllerEndpoint: unable to locate field 'port', which is mandatory in version " + _version);
            } else {
                _object.port = MessageUtil.jsonNodeToUnsignedShort(_portNode, "ControllerEndpoint");
            }
            JsonNode _securityProtocolNode = _node.get("securityProtocol");
            if (_securityProtocolNode == null) {
                throw new RuntimeException("ControllerEndpoint: unable to locate field 'securityProtocol', which is mandatory in version " + _version);
            } else {
                _object.securityProtocol = MessageUtil.jsonNodeToShort(_securityProtocolNode, "ControllerEndpoint");
            }
            return _object;
        }
        public static JsonNode write(ControllerEndpoint _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            _node.set("host", new TextNode(_object.host));
            _node.set("port", new IntNode(_object.port));
            _node.set("securityProtocol", new ShortNode(_object.securityProtocol));
            return _node;
        }
        public static JsonNode write(ControllerEndpoint _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class ControllerFeatureJsonConverter {
        public static ControllerFeature read(JsonNode _node, short _version) {
            ControllerFeature _object = new ControllerFeature();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("ControllerFeature: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("ControllerFeature expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _minSupportedVersionNode = _node.get("minSupportedVersion");
            if (_minSupportedVersionNode == null) {
                throw new RuntimeException("ControllerFeature: unable to locate field 'minSupportedVersion', which is mandatory in version " + _version);
            } else {
                _object.minSupportedVersion = MessageUtil.jsonNodeToShort(_minSupportedVersionNode, "ControllerFeature");
            }
            JsonNode _maxSupportedVersionNode = _node.get("maxSupportedVersion");
            if (_maxSupportedVersionNode == null) {
                throw new RuntimeException("ControllerFeature: unable to locate field 'maxSupportedVersion', which is mandatory in version " + _version);
            } else {
                _object.maxSupportedVersion = MessageUtil.jsonNodeToShort(_maxSupportedVersionNode, "ControllerFeature");
            }
            return _object;
        }
        public static JsonNode write(ControllerFeature _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            _node.set("minSupportedVersion", new ShortNode(_object.minSupportedVersion));
            _node.set("maxSupportedVersion", new ShortNode(_object.maxSupportedVersion));
            return _node;
        }
        public static JsonNode write(ControllerFeature _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
