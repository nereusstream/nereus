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
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.BrokerRegistrationRequestData.*;

public class BrokerRegistrationRequestDataJsonConverter {
    public static BrokerRegistrationRequestData read(JsonNode _node, short _version) {
        BrokerRegistrationRequestData _object = new BrokerRegistrationRequestData();
        JsonNode _brokerIdNode = _node.get("brokerId");
        if (_brokerIdNode == null) {
            throw new RuntimeException("BrokerRegistrationRequestData: unable to locate field 'brokerId', which is mandatory in version " + _version);
        } else {
            _object.brokerId = MessageUtil.jsonNodeToInt(_brokerIdNode, "BrokerRegistrationRequestData");
        }
        JsonNode _clusterIdNode = _node.get("clusterId");
        if (_clusterIdNode == null) {
            throw new RuntimeException("BrokerRegistrationRequestData: unable to locate field 'clusterId', which is mandatory in version " + _version);
        } else {
            if (!_clusterIdNode.isTextual()) {
                throw new RuntimeException("BrokerRegistrationRequestData expected a string type, but got " + _node.getNodeType());
            }
            _object.clusterId = _clusterIdNode.asText();
        }
        JsonNode _incarnationIdNode = _node.get("incarnationId");
        if (_incarnationIdNode == null) {
            throw new RuntimeException("BrokerRegistrationRequestData: unable to locate field 'incarnationId', which is mandatory in version " + _version);
        } else {
            if (!_incarnationIdNode.isTextual()) {
                throw new RuntimeException("BrokerRegistrationRequestData expected a JSON string type, but got " + _node.getNodeType());
            }
            _object.incarnationId = Uuid.fromString(_incarnationIdNode.asText());
        }
        JsonNode _listenersNode = _node.get("listeners");
        if (_listenersNode == null) {
            throw new RuntimeException("BrokerRegistrationRequestData: unable to locate field 'listeners', which is mandatory in version " + _version);
        } else {
            if (!_listenersNode.isArray()) {
                throw new RuntimeException("BrokerRegistrationRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ListenerCollection _collection = new ListenerCollection(_listenersNode.size());
            _object.listeners = _collection;
            for (JsonNode _element : _listenersNode) {
                _collection.add(ListenerJsonConverter.read(_element, _version));
            }
        }
        JsonNode _featuresNode = _node.get("features");
        if (_featuresNode == null) {
            throw new RuntimeException("BrokerRegistrationRequestData: unable to locate field 'features', which is mandatory in version " + _version);
        } else {
            if (!_featuresNode.isArray()) {
                throw new RuntimeException("BrokerRegistrationRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            FeatureCollection _collection = new FeatureCollection(_featuresNode.size());
            _object.features = _collection;
            for (JsonNode _element : _featuresNode) {
                _collection.add(FeatureJsonConverter.read(_element, _version));
            }
        }
        JsonNode _rackNode = _node.get("rack");
        if (_rackNode == null) {
            throw new RuntimeException("BrokerRegistrationRequestData: unable to locate field 'rack', which is mandatory in version " + _version);
        } else {
            if (_rackNode.isNull()) {
                _object.rack = null;
            } else {
                if (!_rackNode.isTextual()) {
                    throw new RuntimeException("BrokerRegistrationRequestData expected a string type, but got " + _node.getNodeType());
                }
                _object.rack = _rackNode.asText();
            }
        }
        JsonNode _isMigratingZkBrokerNode = _node.get("isMigratingZkBroker");
        if (_isMigratingZkBrokerNode == null) {
            if (_version >= 1) {
                throw new RuntimeException("BrokerRegistrationRequestData: unable to locate field 'isMigratingZkBroker', which is mandatory in version " + _version);
            } else {
                _object.isMigratingZkBroker = false;
            }
        } else {
            if (!_isMigratingZkBrokerNode.isBoolean()) {
                throw new RuntimeException("BrokerRegistrationRequestData expected Boolean type, but got " + _node.getNodeType());
            }
            _object.isMigratingZkBroker = _isMigratingZkBrokerNode.asBoolean();
        }
        JsonNode _logDirsNode = _node.get("logDirs");
        if (_logDirsNode == null) {
            if (_version >= 2) {
                throw new RuntimeException("BrokerRegistrationRequestData: unable to locate field 'logDirs', which is mandatory in version " + _version);
            } else {
                _object.logDirs = new ArrayList<Uuid>(0);
            }
        } else {
            if (!_logDirsNode.isArray()) {
                throw new RuntimeException("BrokerRegistrationRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<Uuid> _collection = new ArrayList<Uuid>(_logDirsNode.size());
            _object.logDirs = _collection;
            for (JsonNode _element : _logDirsNode) {
                if (!_element.isTextual()) {
                    throw new RuntimeException("BrokerRegistrationRequestData element expected a JSON string type, but got " + _node.getNodeType());
                }
                _collection.add(Uuid.fromString(_element.asText()));
            }
        }
        JsonNode _previousBrokerEpochNode = _node.get("previousBrokerEpoch");
        if (_previousBrokerEpochNode == null) {
            if (_version >= 3) {
                throw new RuntimeException("BrokerRegistrationRequestData: unable to locate field 'previousBrokerEpoch', which is mandatory in version " + _version);
            } else {
                _object.previousBrokerEpoch = -1L;
            }
        } else {
            _object.previousBrokerEpoch = MessageUtil.jsonNodeToLong(_previousBrokerEpochNode, "BrokerRegistrationRequestData");
        }
        return _object;
    }
    public static JsonNode write(BrokerRegistrationRequestData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("brokerId", new IntNode(_object.brokerId));
        _node.set("clusterId", new TextNode(_object.clusterId));
        _node.set("incarnationId", new TextNode(_object.incarnationId.toString()));
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
        if (_object.rack == null) {
            _node.set("rack", NullNode.instance);
        } else {
            _node.set("rack", new TextNode(_object.rack));
        }
        if (_version >= 1) {
            _node.set("isMigratingZkBroker", BooleanNode.valueOf(_object.isMigratingZkBroker));
        } else {
            if (_object.isMigratingZkBroker) {
                throw new UnsupportedVersionException("Attempted to write a non-default isMigratingZkBroker at version " + _version);
            }
        }
        if (_version >= 2) {
            ArrayNode _logDirsArray = new ArrayNode(JsonNodeFactory.instance);
            for (Uuid _element : _object.logDirs) {
                _logDirsArray.add(new TextNode(_element.toString()));
            }
            _node.set("logDirs", _logDirsArray);
        }
        if (_version >= 3) {
            _node.set("previousBrokerEpoch", new LongNode(_object.previousBrokerEpoch));
        }
        return _node;
    }
    public static JsonNode write(BrokerRegistrationRequestData _object, short _version) {
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
