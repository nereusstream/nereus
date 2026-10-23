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
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Arrays;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.metadata.UserScramCredentialRecord.*;

public class UserScramCredentialRecordJsonConverter {
    public static UserScramCredentialRecord read(JsonNode _node, short _version) {
        UserScramCredentialRecord _object = new UserScramCredentialRecord();
        JsonNode _nameNode = _node.get("name");
        if (_nameNode == null) {
            throw new RuntimeException("UserScramCredentialRecord: unable to locate field 'name', which is mandatory in version " + _version);
        } else {
            if (!_nameNode.isTextual()) {
                throw new RuntimeException("UserScramCredentialRecord expected a string type, but got " + _node.getNodeType());
            }
            _object.name = _nameNode.asText();
        }
        JsonNode _mechanismNode = _node.get("mechanism");
        if (_mechanismNode == null) {
            throw new RuntimeException("UserScramCredentialRecord: unable to locate field 'mechanism', which is mandatory in version " + _version);
        } else {
            _object.mechanism = MessageUtil.jsonNodeToByte(_mechanismNode, "UserScramCredentialRecord");
        }
        JsonNode _saltNode = _node.get("salt");
        if (_saltNode == null) {
            throw new RuntimeException("UserScramCredentialRecord: unable to locate field 'salt', which is mandatory in version " + _version);
        } else {
            _object.salt = MessageUtil.jsonNodeToBinary(_saltNode, "UserScramCredentialRecord");
        }
        JsonNode _storedKeyNode = _node.get("storedKey");
        if (_storedKeyNode == null) {
            throw new RuntimeException("UserScramCredentialRecord: unable to locate field 'storedKey', which is mandatory in version " + _version);
        } else {
            _object.storedKey = MessageUtil.jsonNodeToBinary(_storedKeyNode, "UserScramCredentialRecord");
        }
        JsonNode _serverKeyNode = _node.get("serverKey");
        if (_serverKeyNode == null) {
            throw new RuntimeException("UserScramCredentialRecord: unable to locate field 'serverKey', which is mandatory in version " + _version);
        } else {
            _object.serverKey = MessageUtil.jsonNodeToBinary(_serverKeyNode, "UserScramCredentialRecord");
        }
        JsonNode _iterationsNode = _node.get("iterations");
        if (_iterationsNode == null) {
            throw new RuntimeException("UserScramCredentialRecord: unable to locate field 'iterations', which is mandatory in version " + _version);
        } else {
            _object.iterations = MessageUtil.jsonNodeToInt(_iterationsNode, "UserScramCredentialRecord");
        }
        return _object;
    }
    public static JsonNode write(UserScramCredentialRecord _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("name", new TextNode(_object.name));
        _node.set("mechanism", new ShortNode(_object.mechanism));
        _node.set("salt", new BinaryNode(Arrays.copyOf(_object.salt, _object.salt.length)));
        _node.set("storedKey", new BinaryNode(Arrays.copyOf(_object.storedKey, _object.storedKey.length)));
        _node.set("serverKey", new BinaryNode(Arrays.copyOf(_object.serverKey, _object.serverKey.length)));
        _node.set("iterations", new IntNode(_object.iterations));
        return _node;
    }
    public static JsonNode write(UserScramCredentialRecord _object, short _version) {
        return write(_object, _version, true);
    }
}
