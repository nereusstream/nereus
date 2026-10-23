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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.ControllerRegistrationRequestData.*;

public class ControllerRegistrationRequestDataJsonConverter {
    public static ControllerRegistrationRequestData read(JsonNode _node, short _version) {
        ControllerRegistrationRequestData _object = new ControllerRegistrationRequestData();
        JsonNode _controllerIdNode = _node.get("controllerId");
        if (_controllerIdNode == null) {
            throw new RuntimeException("ControllerRegistrationRequestData: unable to locate field 'controllerId', which is mandatory in version " + _version);
        } else {
            _object.controllerId = MessageUtil.jsonNodeToInt(_controllerIdNode, "ControllerRegistrationRequestData");
        }
        JsonNode _incarnationIdNode = _node.get("incarnationId");
        if (_incarnationIdNode == null) {
            throw new RuntimeException("ControllerRegistrationRequestData: unable to locate field 'incarnationId', which is mandatory in version " + _version);
        } else {
            if (!_incarnationIdNode.isTextual()) {
                throw new RuntimeException("ControllerRegistrationRequestData expected a JSON string type, but got " + _node.getNodeType());
            }
            _object.incarnationId = Uuid.fromString(_incarnationIdNode.asText());
        }
        JsonNode _zkMigrationReadyNode = _node.get("zkMigrationReady");
        if (_zkMigrationReadyNode == null) {
            throw new RuntimeException("ControllerRegistrationRequestData: unable to locate field 'zkMigrationReady', which is mandatory in version " + _version);
        } else {
            if (!_zkMigrationReadyNode.isBoolean()) {
                throw new RuntimeException("ControllerRegistrationRequestData expected Boolean type, but got " + _node.getNodeType());
            }
            _object.zkMigrationReady = _zkMigrationReadyNode.asBoolean();
        }
        JsonNode _listenersNode = _node.get("listeners");
        if (_listenersNode == null) {
            throw new RuntimeException("ControllerRegistrationRequestData: unable to locate field 'listeners', which is mandatory in version " + _version);
        } else {
            if (!_listenersNode.isArray()) {
                throw new RuntimeException("ControllerRegistrationRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ListenerCollection _collection = new ListenerCollection(_listenersNode.size());
            _object.listeners = _collection;
            for (JsonNode _element : _listenersNode) {
                _collection.add(ListenerJsonConverter.read(_element, _version));
            }
        }
        JsonNode _featuresNode = _node.get("features");
        if (_featuresNode == null) {
            throw new RuntimeException("ControllerRegistrationRequestData: unable to locate field 'features', which is mandatory in version " + _version);
        } else {
            if (!_featuresNode.isArray()) {
                throw new RuntimeException("ControllerRegistrationRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            FeatureCollection _collection = new FeatureCollection(_featuresNode.size());
            _object.features = _collection;
            for (JsonNode _element : _featuresNode) {
                _collection.add(FeatureJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(ControllerRegistrationRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("controllerId", new IntNode(_object.controllerId));
        _node.set("incarnationId", new TextNode(_object.incarnationId.toString()));
        _node.set("zkMigrationReady", BooleanNode.valueOf(_object.zkMigrationReady));
        ArrayNode _listenersArray = new ArrayNode(JsonNodeFactory.instance);
        for (Listener _element : _object.listeners) {
            _listenersArray.add(ListenerJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("listeners", _listenersArray);
        ArrayNode _featuresArray = new ArrayNode(JsonNodeFactory.instance);
        for (Feature _element : _object.features) {
            _featuresArray.add(FeatureJsonConverter.write(_element, _version, _serializeRecords));
        }
        _node.set("features", _featuresArray);
        return _node;
    }
    public static JsonNode write(ControllerRegistrationRequestData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class FeatureJsonConverter {
        public static Feature read(JsonNode _node, short _version) {
            Feature _object = new Feature();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("Feature: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("Feature expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _minSupportedVersionNode = _node.get("minSupportedVersion");
            if (_minSupportedVersionNode == null) {
                throw new RuntimeException("Feature: unable to locate field 'minSupportedVersion', which is mandatory in version " + _version);
            } else {
                _object.minSupportedVersion = MessageUtil.jsonNodeToShort(_minSupportedVersionNode, "Feature");
            }
            JsonNode _maxSupportedVersionNode = _node.get("maxSupportedVersion");
            if (_maxSupportedVersionNode == null) {
                throw new RuntimeException("Feature: unable to locate field 'maxSupportedVersion', which is mandatory in version " + _version);
            } else {
                _object.maxSupportedVersion = MessageUtil.jsonNodeToShort(_maxSupportedVersionNode, "Feature");
            }
            return _object;
        }
        public static JsonNode write(Feature _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            _node.set("minSupportedVersion", new ShortNode(_object.minSupportedVersion));
            _node.set("maxSupportedVersion", new ShortNode(_object.maxSupportedVersion));
            return _node;
        }
        public static JsonNode write(Feature _object, short _version) {
            return write(_object, _version, true);
        }
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
            JsonNode _securityProtocolNode = _node.get("securityProtocol");
            if (_securityProtocolNode == null) {
                throw new RuntimeException("Listener: unable to locate field 'securityProtocol', which is mandatory in version " + _version);
            } else {
                _object.securityProtocol = MessageUtil.jsonNodeToShort(_securityProtocolNode, "Listener");
            }
            return _object;
        }
        public static JsonNode write(Listener _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            _node.set("host", new TextNode(_object.host));
            _node.set("port", new IntNode(_object.port));
            _node.set("securityProtocol", new ShortNode(_object.securityProtocol));
            return _node;
        }
        public static JsonNode write(Listener _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
