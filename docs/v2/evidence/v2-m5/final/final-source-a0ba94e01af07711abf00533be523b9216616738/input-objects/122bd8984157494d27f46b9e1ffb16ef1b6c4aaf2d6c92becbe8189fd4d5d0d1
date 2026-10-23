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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.NullableStructMessageData.*;

public class NullableStructMessageDataJsonConverter {
    public static NullableStructMessageData read(JsonNode _node, short _version) {
        NullableStructMessageData _object = new NullableStructMessageData();
        JsonNode _nullableStructNode = _node.get("nullableStruct");
        if (_nullableStructNode == null) {
            throw new RuntimeException("NullableStructMessageData: unable to locate field 'nullableStruct', which is mandatory in version " + _version);
        } else {
            if (_nullableStructNode.isNull()) {
                _object.nullableStruct = null;
            } else {
                _object.nullableStruct = MyStructJsonConverter.read(_nullableStructNode, _version);
            }
        }
        JsonNode _nullableStruct2Node = _node.get("nullableStruct2");
        if (_nullableStruct2Node == null) {
            throw new RuntimeException("NullableStructMessageData: unable to locate field 'nullableStruct2', which is mandatory in version " + _version);
        } else {
            if (_nullableStruct2Node.isNull()) {
                _object.nullableStruct2 = null;
            } else {
                _object.nullableStruct2 = MyStruct2JsonConverter.read(_nullableStruct2Node, _version);
            }
        }
        JsonNode _nullableStruct3Node = _node.get("nullableStruct3");
        if (_nullableStruct3Node == null) {
            _object.nullableStruct3 = null;
        } else {
            if (_nullableStruct3Node.isNull()) {
                _object.nullableStruct3 = null;
            } else {
                _object.nullableStruct3 = MyStruct3JsonConverter.read(_nullableStruct3Node, _version);
            }
        }
        JsonNode _nullableStruct4Node = _node.get("nullableStruct4");
        if (_nullableStruct4Node == null) {
            _object.nullableStruct4 = new MyStruct4();
        } else {
            if (_nullableStruct4Node.isNull()) {
                _object.nullableStruct4 = null;
            } else {
                _object.nullableStruct4 = MyStruct4JsonConverter.read(_nullableStruct4Node, _version);
            }
        }
        return _object;
    }
    public static JsonNode write(NullableStructMessageData _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        if (_object.nullableStruct == null) {
            _node.set("nullableStruct", NullNode.instance);
        } else {
            _node.set("nullableStruct", MyStructJsonConverter.write(_object.nullableStruct, _version, _serializeRecords));
        }
        if (_object.nullableStruct2 == null) {
            _node.set("nullableStruct2", NullNode.instance);
        } else {
            _node.set("nullableStruct2", MyStruct2JsonConverter.write(_object.nullableStruct2, _version, _serializeRecords));
        }
        if (_version >= 1) {
            if (_object.nullableStruct3 != null) {
                _node.set("nullableStruct3", MyStruct3JsonConverter.write(_object.nullableStruct3, _version, _serializeRecords));
            }
        } else {
            if (_object.nullableStruct3 != null) {
                throw new UnsupportedVersionException("Attempted to write a non-default nullableStruct3 at version " + _version);
            }
        }
        if (_version >= 1) {
            if (_object.nullableStruct4 == null || !_object.nullableStruct4.equals(new MyStruct4())) {
                if (_object.nullableStruct4 == null) {
                    _node.set("nullableStruct4", NullNode.instance);
                } else {
                    _node.set("nullableStruct4", MyStruct4JsonConverter.write(_object.nullableStruct4, _version, _serializeRecords));
                }
            }
        } else {
            if (_object.nullableStruct4 == null || !_object.nullableStruct4.equals(new MyStruct4())) {
                throw new UnsupportedVersionException("Attempted to write a non-default nullableStruct4 at version " + _version);
            }
        }
        return _node;
    }
    public static JsonNode write(NullableStructMessageData _object, short _version) {
        return write(_object, _version, true);
    }
    
    public static class MyStructJsonConverter {
        public static MyStruct read(JsonNode _node, short _version) {
            MyStruct _object = new MyStruct();
            JsonNode _myStringNode = _node.get("myString");
            if (_myStringNode == null) {
                throw new RuntimeException("MyStruct: unable to locate field 'myString', which is mandatory in version " + _version);
            } else {
                if (!_myStringNode.isTextual()) {
                    throw new RuntimeException("MyStruct expected a string type, but got " + _node.getNodeType());
                }
                _object.myString = _myStringNode.asText();
            }
            JsonNode _myIntNode = _node.get("myInt");
            if (_myIntNode == null) {
                throw new RuntimeException("MyStruct: unable to locate field 'myInt', which is mandatory in version " + _version);
            } else {
                _object.myInt = MessageUtil.jsonNodeToInt(_myIntNode, "MyStruct");
            }
            return _object;
        }
        public static JsonNode write(MyStruct _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("myString", new TextNode(_object.myString));
            _node.set("myInt", new IntNode(_object.myInt));
            return _node;
        }
        public static JsonNode write(MyStruct _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class MyStruct2JsonConverter {
        public static MyStruct2 read(JsonNode _node, short _version) {
            MyStruct2 _object = new MyStruct2();
            JsonNode _myStringNode = _node.get("myString");
            if (_myStringNode == null) {
                throw new RuntimeException("MyStruct2: unable to locate field 'myString', which is mandatory in version " + _version);
            } else {
                if (!_myStringNode.isTextual()) {
                    throw new RuntimeException("MyStruct2 expected a string type, but got " + _node.getNodeType());
                }
                _object.myString = _myStringNode.asText();
            }
            JsonNode _myIntNode = _node.get("myInt");
            if (_myIntNode == null) {
                throw new RuntimeException("MyStruct2: unable to locate field 'myInt', which is mandatory in version " + _version);
            } else {
                _object.myInt = MessageUtil.jsonNodeToInt(_myIntNode, "MyStruct2");
            }
            return _object;
        }
        public static JsonNode write(MyStruct2 _object, short _version, boolean _serializeRecords) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("myString", new TextNode(_object.myString));
            _node.set("myInt", new IntNode(_object.myInt));
            return _node;
        }
        public static JsonNode write(MyStruct2 _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class MyStruct3JsonConverter {
        public static MyStruct3 read(JsonNode _node, short _version) {
            MyStruct3 _object = new MyStruct3();
            if (_version < 1) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of MyStruct3");
            }
            JsonNode _myStringNode = _node.get("myString");
            if (_myStringNode == null) {
                throw new RuntimeException("MyStruct3: unable to locate field 'myString', which is mandatory in version " + _version);
            } else {
                if (!_myStringNode.isTextual()) {
                    throw new RuntimeException("MyStruct3 expected a string type, but got " + _node.getNodeType());
                }
                _object.myString = _myStringNode.asText();
            }
            JsonNode _myIntNode = _node.get("myInt");
            if (_myIntNode == null) {
                throw new RuntimeException("MyStruct3: unable to locate field 'myInt', which is mandatory in version " + _version);
            } else {
                _object.myInt = MessageUtil.jsonNodeToInt(_myIntNode, "MyStruct3");
            }
            return _object;
        }
        public static JsonNode write(MyStruct3 _object, short _version, boolean _serializeRecords) {
            if (_version < 1) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of MyStruct3");
            }
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("myString", new TextNode(_object.myString));
            _node.set("myInt", new IntNode(_object.myInt));
            return _node;
        }
        public static JsonNode write(MyStruct3 _object, short _version) {
            return write(_object, _version, true);
        }
    }
    
    public static class MyStruct4JsonConverter {
        public static MyStruct4 read(JsonNode _node, short _version) {
            MyStruct4 _object = new MyStruct4();
            if (_version < 1) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of MyStruct4");
            }
            JsonNode _myStringNode = _node.get("myString");
            if (_myStringNode == null) {
                throw new RuntimeException("MyStruct4: unable to locate field 'myString', which is mandatory in version " + _version);
            } else {
                if (!_myStringNode.isTextual()) {
                    throw new RuntimeException("MyStruct4 expected a string type, but got " + _node.getNodeType());
                }
                _object.myString = _myStringNode.asText();
            }
            JsonNode _myIntNode = _node.get("myInt");
            if (_myIntNode == null) {
                throw new RuntimeException("MyStruct4: unable to locate field 'myInt', which is mandatory in version " + _version);
            } else {
                _object.myInt = MessageUtil.jsonNodeToInt(_myIntNode, "MyStruct4");
            }
            return _object;
        }
        public static JsonNode write(MyStruct4 _object, short _version, boolean _serializeRecords) {
            if (_version < 1) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of MyStruct4");
            }
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("myString", new TextNode(_object.myString));
            _node.set("myInt", new IntNode(_object.myInt));
            return _node;
        }
        public static JsonNode write(MyStruct4 _object, short _version) {
            return write(_object, _version, true);
        }
    }
}
