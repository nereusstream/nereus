/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.nwg1;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Small strict RFC-8785 subset used by the integer/string/bool/null NWG1 manifest schema. */
final class StrictJcsV1 {
    private StrictJcsV1() {}

    static Object parseCanonical(byte[] bytes) {
        if (bytes.length >= 3 && bytes[0] == (byte) 0xef && bytes[1] == (byte) 0xbb && bytes[2] == (byte) 0xbf) {
            throw new IllegalArgumentException("JCS must not contain a BOM");
        }
        String text;
        try {
            text = StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("invalid UTF-8", e);
        }
        Parser parser = new Parser(text);
        Object value = parser.value();
        if (!parser.eof()) {
            throw new IllegalArgumentException("JCS trailing bytes");
        }
        byte[] canonical = encode(value).getBytes(StandardCharsets.UTF_8);
        if (!java.security.MessageDigest.isEqual(bytes, canonical)) {
            throw new IllegalArgumentException("not canonical JCS");
        }
        return value;
    }

    static String encode(Object value) {
        StringBuilder out = new StringBuilder();
        write(value, out);
        return out.toString();
    }

    @SuppressWarnings("unchecked")
    private static void write(Object value, StringBuilder out) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof Boolean bool) {
            out.append(bool);
        } else if (value instanceof Long number) {
            out.append(number);
        } else if (value instanceof String string) {
            string(string, out);
        } else if (value instanceof List<?> list) {
            out.append('[');
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    out.append(',');
                }
                write(list.get(i), out);
            }
            out.append(']');
        } else if (value instanceof Map<?, ?> map) {
            out.append('{');
            boolean first = true;
            for (var entry : new TreeMap<>((Map<String, Object>) map).entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                string(entry.getKey(), out);
                out.append(':');
                write(entry.getValue(), out);
            }
            out.append('}');
        } else {
            throw new IllegalArgumentException("unsupported JCS value");
        }
    }

    private static void string(String value, StringBuilder out) {
        out.append('"');
        value.codePoints().forEach(code -> {
            switch (code) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case 8 -> out.append("\\b");
                case 9 -> out.append("\\t");
                case 10 -> out.append("\\n");
                case 12 -> out.append("\\f");
                case 13 -> out.append("\\r");
                default -> {
                    if (code < 0x20) {
                        out.append(String.format("\\u%04x", code));
                    } else {
                        out.appendCodePoint(code);
                    }
                }
            }
        });
        out.append('"');
    }

    private static final class Parser {
        private final String text;
        private int index;

        Parser(String text) {
            this.text = text;
        }

        Object value() {
            if (eof()) {
                throw new IllegalArgumentException("truncated JCS");
            }
            return switch (text.charAt(index)) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't' -> literal("true", true);
                case 'f' -> literal("false", false);
                case 'n' -> literal("null", null);
                default -> integer();
            };
        }

        Map<String, Object> object() {
            expect('{');
            Map<String, Object> result = new LinkedHashMap<>();
            if (take('}')) {
                return result;
            }
            while (true) {
                if (peek() != '"') {
                    throw new IllegalArgumentException("object key must be string");
                }
                String key = string();
                expect(':');
                if (result.containsKey(key)) {
                    throw new IllegalArgumentException("duplicate object key");
                }
                result.put(key, value());
                if (take('}')) {
                    return result;
                }
                expect(',');
            }
        }

        List<Object> array() {
            expect('[');
            List<Object> result = new ArrayList<>();
            if (take(']')) {
                return result;
            }
            while (true) {
                result.add(value());
                if (take(']')) {
                    return result;
                }
                expect(',');
            }
        }

        String string() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (!eof()) {
                char next = text.charAt(index++);
                if (next == '"') {
                    return out.toString();
                }
                if (next < 0x20) {
                    throw new IllegalArgumentException("unescaped control");
                }
                if (next != '\\') {
                    out.append(next);
                    continue;
                }
                if (eof()) {
                    throw new IllegalArgumentException("truncated escape");
                }
                char escape = text.charAt(index++);
                switch (escape) {
                    case '"', '\\', '/' -> out.append(escape);
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'u' -> out.append((char) Integer.parseInt(read(4), 16));
                    default -> throw new IllegalArgumentException("unknown escape");
                }
            }
            throw new IllegalArgumentException("unterminated string");
        }

        Long integer() {
            int start = index;
            if (take('-') && eof()) {
                throw new IllegalArgumentException("truncated integer");
            }
            if (take('0')) {
                if (!eof() && Character.isDigit(peek())) {
                    throw new IllegalArgumentException("leading zero");
                }
            } else {
                if (eof() || peek() < '1' || peek() > '9') {
                    throw new IllegalArgumentException("not an integer");
                }
                while (!eof() && Character.isDigit(peek())) {
                    index++;
                }
            }
            if (!eof() && (peek() == '.' || peek() == 'e' || peek() == 'E')) {
                throw new IllegalArgumentException("manifest numbers must be integers");
            }
            return Long.parseLong(text.substring(start, index));
        }

        Object literal(String token, Object value) {
            if (!text.startsWith(token, index)) {
                throw new IllegalArgumentException("bad literal");
            }
            index += token.length();
            return value;
        }

        String read(int length) {
            if (index + length > text.length()) {
                throw new IllegalArgumentException("truncated unicode escape");
            }
            String result = text.substring(index, index + length);
            index += length;
            return result;
        }

        void expect(char expected) {
            if (eof() || text.charAt(index++) != expected) {
                throw new IllegalArgumentException("expected " + expected);
            }
        }

        boolean take(char value) {
            if (!eof() && text.charAt(index) == value) {
                index++;
                return true;
            }
            return false;
        }

        char peek() {
            return text.charAt(index);
        }

        boolean eof() {
            return index == text.length();
        }
    }
}
