/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Seals the production reparser output together with its exact one-test, zero-error JUnit XML. */
public final class M3AllocatorVerificationSealMain {
    static final String TEST_CLASS =
            "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3AllocatorRawEvidenceVerificationTest";
    static final String TEST_CASE = "recomputesNarsNaeaJunitAndExactSourceArtifacts()";
    static final String TEST_XML = "TEST-" + TEST_CLASS + ".xml";
    static final String SELF_HASH_RULE = "SHA256_OF_EXACT_UTF8_WITH_SELF_SHA256_64_ZERO_HEX";
    private static final String ZERO_SHA = "0".repeat(64);

    private M3AllocatorVerificationSealMain() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("expected raw verification JSON, exact verifier JUnit XML, and output");
        }
        Path rawVerification = regular(arguments[0]);
        Path junitXml = regular(arguments[1]);
        Path output = Path.of(arguments[2]).toAbsolutePath().normalize();
        if (!"raw-verification-payload.json".equals(rawVerification.getFileName().toString())
                || !TEST_XML.equals(junitXml.getFileName().toString())
                || !"raw-verification.json".equals(output.getFileName().toString())) {
            throw new IllegalArgumentException("allocator sealed verification path basenames differ");
        }
        if (Files.exists(output)) {
            throw new IllegalStateException("allocator sealed verification output already exists: " + output);
        }

        byte[] rawBytes = Files.readAllBytes(rawVerification);
        String rawJson = decodeUtf8(rawBytes);
        if (!rawJson.startsWith("{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_RAW_RECOMPUTATION_V1\"")
                || !rawJson.endsWith("}\n")
                || !rawJson.contains("\"status\":\"PASS_RAW_RECOMPUTED\"")) {
            throw new IllegalArgumentException("allocator raw verification JSON is not the exact recomputation schema");
        }
        verifyRawSelfHash(rawJson);
        JUnitIdentity junit = parseJUnit(Files.readAllBytes(junitXml));
        String embeddedRaw = rawJson.substring(0, rawJson.length() - 1);
        String zeroed = "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_SEALED_VERIFICATION_V1\","
                + "\"selfSha256\":\""
                + ZERO_SHA
                + "\",\"selfHashRule\":\""
                + SELF_HASH_RULE
                + "\",\"rawVerification\":"
                + embeddedRaw
                + ",\"rawVerificationBytes\":"
                + rawBytes.length
                + ",\"rawVerificationSha256\":\""
                + sha256(rawBytes)
                + "\",\"verifierJUnit\":{\"basename\":\""
                + junitXml.getFileName()
                + "\",\"bytes\":"
                + Files.size(junitXml)
                + ",\"sha256\":\""
                + sha256(Files.readAllBytes(junitXml))
                + "\",\"tests\":1,\"failures\":0,\"errors\":0,\"skips\":0,\"testClass\":\""
                + junit.testClass()
                + "\",\"testCase\":\""
                + junit.testCase()
                + "\"}}\n";
        String selfSha = sha256(zeroed.getBytes(StandardCharsets.UTF_8));
        int selfOffset = zeroed.indexOf(ZERO_SHA);
        String sealed = zeroed.substring(0, selfOffset)
                + selfSha
                + zeroed.substring(selfOffset + ZERO_SHA.length());
        Files.writeString(
                output,
                sealed,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
    }

    private static JUnitIdentity parseJUnit(byte[] xml) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);
        Element suite = factory.newDocumentBuilder()
                .parse(new ByteArrayInputStream(xml))
                .getDocumentElement();
        if (!"testsuite".equals(suite.getTagName())
                || !"1".equals(suite.getAttribute("tests"))
                || !"0".equals(suite.getAttribute("failures"))
                || !"0".equals(suite.getAttribute("errors"))
                || !"0".equals(suite.getAttribute("skipped"))) {
            throw new IllegalArgumentException("allocator verifier JUnit suite counts differ from exact 1/0/0/0");
        }
        Element testcase = null;
        NodeList children = suite.getChildNodes();
        for (int index = 0; index < children.getLength(); index++) {
            Node child = children.item(index);
            if (child.getNodeType() == Node.ELEMENT_NODE
                    && "testcase".equals(((Element) child).getTagName())) {
                if (testcase != null) {
                    throw new IllegalArgumentException("allocator verifier JUnit has more than one testcase");
                }
                testcase = (Element) child;
            }
        }
        if (testcase == null
                || !TEST_CLASS.equals(testcase.getAttribute("classname"))
                || !TEST_CASE.equals(testcase.getAttribute("name"))) {
            throw new IllegalArgumentException("allocator verifier JUnit testcase identity differs");
        }
        for (Node child = testcase.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                throw new IllegalArgumentException("allocator verifier JUnit testcase has a terminal child");
            }
        }
        return new JUnitIdentity(TEST_CLASS, TEST_CASE);
    }

    private static void verifyRawSelfHash(String rawJson) throws Exception {
        String marker = "\"selfSha256\":\"";
        int valueStart = rawJson.indexOf(marker);
        int valueEnd = valueStart < 0 ? -1 : valueStart + marker.length() + 64;
        if (valueStart < 0
                || valueEnd >= rawJson.length()
                || rawJson.charAt(valueEnd) != '"'
                || rawJson.indexOf(marker, valueEnd) >= 0
                || !rawJson.substring(valueEnd + 1)
                        .startsWith(",\"selfHashRule\":\"" + M3AllocatorEvidenceVerifyMain.SELF_HASH_RULE + "\"")) {
            throw new IllegalArgumentException("allocator raw verification self-hash fields differ");
        }
        valueStart += marker.length();
        String claimed = rawJson.substring(valueStart, valueEnd);
        try {
            HexFormat.of().parseHex(claimed);
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException("allocator raw verification self hash is not hexadecimal", malformed);
        }
        String zeroed = rawJson.substring(0, valueStart)
                + ZERO_SHA
                + rawJson.substring(valueEnd);
        if (!claimed.equals(sha256(zeroed.getBytes(StandardCharsets.UTF_8)))) {
            throw new IllegalArgumentException("allocator raw verification self hash differs");
        }
    }

    private static Path regular(String value) {
        Path path = Path.of(value).toAbsolutePath().normalize();
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("allocator verification input is not an exact regular file: " + path);
        }
        return path;
    }

    private static String decodeUtf8(byte[] bytes) throws Exception {
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private record JUnitIdentity(String testClass, String testCase) {}
}
