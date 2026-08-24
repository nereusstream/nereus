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

package com.nereusstream.domain.registry.allocator;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Strict XXE-disabled JUnit XML counter used by both the NAEA writer and production raw parser. */
final class AllocatorJUnitEvidenceV1 {
    private AllocatorJUnitEvidenceV1() {}

    static Counts parse(InputStream xml) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            Element root = factory.newDocumentBuilder().parse(xml).getDocumentElement();
            List<Element> suites = new ArrayList<>();
            if ("testsuite".equals(root.getTagName())) {
                suites.add(root);
            } else if ("testsuites".equals(root.getTagName())) {
                for (Element child : directElements(root)) {
                    if (!"testsuite".equals(child.getTagName())) {
                        throw invalid("allocator JUnit testsuites contains an unknown direct element");
                    }
                    suites.add(child);
                }
            } else {
                throw invalid("allocator JUnit XML root must be testsuite or testsuites");
            }
            if (suites.isEmpty()) {
                throw invalid("allocator JUnit XML has no test suite");
            }
            Counts total = new Counts(0, 0, 0, 0);
            for (Element suite : suites) {
                total = total.add(parseSuite(suite));
            }
            if (total.tests <= 0) {
                throw invalid("allocator JUnit XML has zero tests");
            }
            if ("testsuites".equals(root.getTagName()) && root.hasAttribute("tests")) {
                Counts declared = declared(root);
                if (!declared.equals(total)) {
                    throw invalid("allocator JUnit testsuites aggregate counts differ from child suites");
                }
            }
            return total;
        } catch (AllocatorProtocolException error) {
            throw error;
        } catch (Exception error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator JUnit XML could not be parsed securely",
                    error);
        }
    }

    private static Counts parseSuite(Element suite) {
        Counts declared = declared(suite);
        long tests = 0;
        long failures = 0;
        long errors = 0;
        long skipped = 0;
        for (Element child : directElements(suite)) {
            if ("testcase".equals(child.getTagName())) {
                tests++;
                int terminal = 0;
                for (Element result : directElements(child)) {
                    switch (result.getTagName()) {
                        case "failure" -> {
                            failures++;
                            terminal++;
                        }
                        case "error" -> {
                            errors++;
                            terminal++;
                        }
                        case "skipped" -> {
                            skipped++;
                            terminal++;
                        }
                        case "system-out", "system-err" -> {
                            // Diagnostic text is exact attachment content but not a result counter.
                        }
                        default -> throw invalid("allocator JUnit testcase contains an unknown direct element");
                    }
                }
                if (terminal > 1) {
                    throw invalid("allocator JUnit testcase has more than one terminal result");
                }
            } else if (!"properties".equals(child.getTagName())
                    && !"system-out".equals(child.getTagName())
                    && !"system-err".equals(child.getTagName())) {
                throw invalid("allocator JUnit testsuite contains an unknown direct element");
            }
        }
        Counts observed = new Counts(tests, failures, errors, skipped);
        if (!declared.equals(observed)) {
            throw invalid("allocator JUnit suite declared counts differ from testcase results");
        }
        return observed;
    }

    private static Counts declared(Element element) {
        return new Counts(
                decimal(element, "tests"),
                decimal(element, "failures"),
                decimal(element, "errors"),
                decimal(element, "skipped"));
    }

    private static long decimal(Element element, String attribute) {
        String value = element.getAttribute(attribute);
        if (value.isEmpty() || !value.chars().allMatch(Character::isDigit)) {
            throw invalid("allocator JUnit count is absent or non-decimal");
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException error) {
            throw invalid("allocator JUnit count exceeds signed 64-bit bounds");
        }
    }

    private static List<Element> directElements(Element parent) {
        List<Element> elements = new ArrayList<>();
        NodeList children = parent.getChildNodes();
        for (int index = 0; index < children.getLength(); index++) {
            Node child = children.item(index);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                elements.add((Element) child);
            }
        }
        return elements;
    }

    private static AllocatorProtocolException invalid(String message) {
        return AllocatorSelectionReceiptV1.invalid(message);
    }

    record Counts(long tests, long failures, long errors, long skipped) {
        private Counts add(Counts other) {
            try {
                return new Counts(
                        Math.addExact(tests, other.tests),
                        Math.addExact(failures, other.failures),
                        Math.addExact(errors, other.errors),
                        Math.addExact(skipped, other.skipped));
            } catch (ArithmeticException error) {
                throw invalid("allocator JUnit aggregate count overflows");
            }
        }

        boolean zeroFailureErrorSkip() {
            return tests > 0 && failures == 0 && errors == 0 && skipped == 0;
        }
    }
}
