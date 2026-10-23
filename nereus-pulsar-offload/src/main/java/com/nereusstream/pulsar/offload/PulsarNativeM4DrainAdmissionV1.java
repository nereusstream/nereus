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

package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

/** Explicit current-source Native SDK drain admission; neither expiry nor M5/physical-delete authority. */
public final class PulsarNativeM4DrainAdmissionV1 {
    private static final List<String> RUNTIME_TYPES = List.of(
            "org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl",
            "org.apache.pulsar.common.protocol.Commands",
            "org.apache.pulsar.metadata.api.extended.MetadataStoreExtended",
            "org.apache.bookkeeper.client.BookKeeper",
            "io.oxia.client.OxiaClientBuilderImpl",
            "io.oxia.client.api.OxiaClientBuilder",
            "software.amazon.awssdk.services.s3.S3Client",
            "software.amazon.awssdk.core.SdkClient",
            "software.amazon.awssdk.http.apache5.Apache5HttpClient",
            "com.nereusstream.domain.bytes.Sha256Digest",
            "com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1",
            "com.nereusstream.metadata.oxia.v2.retention.OxiaBindingLifecycleMetadataStoreV2",
            "com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1",
            "com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2",
            "com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1",
            "com.nereusstream.storage.object.s3.S3M5PhysicalNamespaceAuthorityV2",
            "com.nereusstream.pulsar.offload.PulsarNativeM4DrainAdmissionV1");
    private static final List<String> REQUIRED_MARKERS = List.of(
            "NATIVE_PULSAR_BINDING_SOURCE",
            "completeNativeDataView=true",
            "coldProperties=true",
            "NATIVE_PULSAR_IMMUTABLE_GET",
            "NATIVE_ORDINARY_CALLBACK_DRAIN",
            "failedCallback=true",
            "NATIVE_PULSAR_OWNER_CANCELLED_OPEN_DRAIN",
            "NATIVE_PULSAR_CANCELLED_RANGE_DRAIN",
            "NATIVE_PULSAR_FENCED_OWNER_DRAIN",
            "fenceFailureRetained=true",
            "observation=delivered",
            "observation=cancelled",
            "observation=completed",
            "NATIVE_PULSAR_OWNED_CAPTURE_CANCELLED_FALLBACK",
            "nativeBookKeeperReadCompletions=0",
            "NATIVE_PULSAR_PREFERRED_ONLY_SOURCE",
            "bookKeeperMetadataMissing=true",
            "offloadedBookKeeperNotSelected=true",
            "actualObjectOnly=true",
            "NATIVE_CREATE_DRAIN actualSdkCreated=true callerTimeout=true cursor=false",
            "NATIVE_CREATE_DRAIN actualSdkCreated=true callerTimeout=true cursor=true",
            "closeWaitsForActualResponse=true observerCannotFree=true lateHandleClosed=true",
            "orphanRetained=true physicalDeleteAuthority=false wholeM5Authority=false");

    private final Sha256Digest receipt;
    private final Sha256Digest inputs;
    private final Sha256Digest runtime;
    private final Sha256Digest configuration;

    private PulsarNativeM4DrainAdmissionV1(
            Sha256Digest receipt, Sha256Digest inputs, Sha256Digest runtime, Sha256Digest configuration) {
        this.receipt = receipt;
        this.inputs = inputs;
        this.runtime = runtime;
        this.configuration = configuration;
    }

    /** The controller pins an independently verified capsule digest; stored ADMITTED bytes cannot call this path. */
    public static PulsarNativeM4DrainAdmissionV1 load(Path capsule, Sha256Digest expectedReceipt) throws Exception {
        byte[] bytes = bounded(capsule, 2_097_152);
        if (!sha(bytes).equals(expectedReceipt) || expectedReceipt.isZero()) {
            throw new IllegalArgumentException("Native M4 conformance capsule differs from its explicit admission");
        }
        var values = new Properties();
        values.load(new ByteArrayInputStream(bytes));
        require(values, "schema", "NEREUS_NATIVE_PULSAR_M4_SDK_DRAIN_V1");
        require(values, "authority", "ACTUAL_NATIVE_PULSAR_BK_OBJECT_OXIA");
        require(values, "expiryAuthority", "false");
        require(values, "wholeM5Authority", "false");
        require(values, "physicalDeleteAuthority", "false");
        require(values, "bookKeeperImageId", "sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605");
        require(values, "minioImageId", "sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253");
        require(values, "oxiaImageId", "sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da");
        int count = count(values, "source.count", 4096);
        var sourceDigests = new TreeMap<String, String>();
        for (int index = 0; index < count; index++) {
            String key = "source." + index;
            var path = Path.of(required(values, key + ".path")).toRealPath();
            String actual = sha(bounded(path, 16_777_216)).toHex();
            require(values, key + ".sha256", actual);
            if (sourceDigests.put(path.toString(), actual) != null) {
                throw new IllegalArgumentException("Native M4 source closure contains a duplicate");
            }
        }
        // Required production and scenario sources cannot be omitted from a caller's capsule.
        for (String suffix : List.of(
                "/ManagedLedgerImpl.java",
                "/ManagedCursorImpl.java",
                "/EntryImpl.java",
                "/DualSourceReadHandle.java",
                "/SourceSafeLedgerOffloader.java",
                "/MetaStoreImpl.java",
                "/PulsarNativeBindingV1.java",
                "/PulsarNativeBindingReadViewV1.java",
                "/PulsarNativeM4ControlV1.java",
                "/PulsarNativeM4ReadOwnerV1.java",
                "/PulsarNativeM4DrainAdmissionV1.java",
                "/S3PulsarOffloadObjectStoreV1.java",
                "/NereusPulsarLedgerOffloaderV1.java",
                "/BindingReadAsyncExecutorV1.java",
                "/BindingReadHazardPoolV1.java",
                "/M4ReadControlCoordinatorV1.java",
                "/M5NativePulsarOffloadRealTest.java",
                "/PulsarNativeM4FallbackRetirementV1.java",
                "/PulsarNativeM4SourceMapV1.java",
                "/PulsarNativeM4ColdRecoveryV1.java",
                "/PulsarNativeReadCellBudgetV1.java",
                "/PulsarNativeReadCellAuthorityV1.java",
                "/PulsarNativeSourcePreparationV1.java",
                "/M5S3PulsarObjectStoreDrainTest.java")) {
            if (sourceDigests.keySet().stream().noneMatch(path -> path.endsWith(suffix))) {
                throw new IllegalArgumentException("Native M4 source closure omits " + suffix);
            }
        }
        var actualRuntime = runtimeFingerprint();
        for (var entry : actualRuntime.entrySet()) {
            require(values, "runtime." + entry.getKey(), entry.getValue());
        }
        boolean nativeSuite = false;
        boolean providerSuite = false;
        int suites = count(values, "suite.count", 64);
        for (int index = 0; index < suites; index++) {
            String key = "suite." + index;
            var xml = bounded(Path.of(required(values, key + ".path")), 8_388_608);
            require(values, key + ".sha256", sha(xml).toHex());
            var parser = DocumentBuilderFactory.newInstance();
            parser.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            parser.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            parser.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            var suite = parser.newDocumentBuilder()
                    .parse(new ByteArrayInputStream(xml))
                    .getDocumentElement();
            if (!suite.getTagName().equals("testsuite")
                    || Integer.parseInt(suite.getAttribute("tests")) <= 0
                    || !suite.getAttribute("failures").equals("0")
                    || !suite.getAttribute("errors").equals("0")
                    || !suite.getAttribute("skipped").equals("0")) {
                throw new IllegalArgumentException("Native M4 conformance suite is not an executed success");
            }
            if (suite.getAttribute("name").endsWith(".M5NativePulsarOffloadRealTest")) {
                nativeSuite = suite.getAttribute("tests").equals("5");
                String output = suite.getTextContent();
                if (REQUIRED_MARKERS.stream().anyMatch(marker -> !output.contains(marker))) {
                    throw new IllegalArgumentException("Native M4 conformance lacks actual Native drain controls");
                }
            }
            if (suite.getAttribute("name").endsWith(".M5S3PulsarObjectStoreDrainTest")) {
                providerSuite = suite.getAttribute("tests").equals("5");
            }
        }
        if (!nativeSuite || !providerSuite) {
            throw new IllegalArgumentException("Native M4 admission lacks actual Native and SDK drain results");
        }
        return new PulsarNativeM4DrainAdmissionV1(
                expectedReceipt,
                digest(sourceDigests),
                digest(actualRuntime),
                parse(required(values, "configuration")));
    }

    void requireConfiguration(
            PulsarNativeBindingReadViewV1 view,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader) {
        if (!configuration.equals(view.readAdmissionConfiguration(bookKeeper, offloader))) {
            throw new IllegalArgumentException("Native M4 actual backend configuration differs from conformance");
        }
    }

    Sha256Digest receipt() {
        return receipt;
    }

    Sha256Digest inputs() {
        return inputs;
    }

    Sha256Digest runtime() {
        return runtime;
    }

    Sha256Digest configuration() {
        return configuration;
    }

    /** Complete classes directory/JAR closure plus the selected class bytes under the actual loader. */
    public static Map<String, String> runtimeFingerprint() throws Exception {
        var values = new TreeMap<String, String>();
        var roots = new TreeMap<Path, String>();
        for (String name : RUNTIME_TYPES) {
            var type = Class.forName(name, false, PulsarNativeM4DrainAdmissionV1.class.getClassLoader());
            var root = Path.of(type.getProtectionDomain()
                            .getCodeSource()
                            .getLocation()
                            .toURI())
                    .toRealPath();
            String rootHash = roots.get(root);
            if (rootHash == null) {
                rootHash = hashCodeRoot(root);
                roots.put(root, rootHash);
            }
            values.put(name, rootHash);
            try (var stream = type.getResourceAsStream("/" + name.replace('.', '/') + ".class")) {
                if (stream == null) {
                    throw new IllegalArgumentException("Native M4 runtime class is absent: " + name);
                }
                values.put(name + ".class", sha(stream.readAllBytes()).toHex());
            }
        }
        return Map.copyOf(values);
    }

    private static String hashCodeRoot(Path root) throws Exception {
        if (!Files.isDirectory(root)) {
            return sha(bounded(root, 134_217_728)).toHex();
        }
        var digest = MessageDigest.getInstance("SHA-256");
        try (var files = Files.walk(root)) {
            var classes = files.filter(path -> path.toString().endsWith(".class"))
                    .sorted()
                    .toList();
            if (classes.isEmpty()) {
                throw new IllegalArgumentException("Native M4 runtime closure is empty");
            }
            for (var path : classes) {
                digest.update(root.relativize(path).toString().getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0);
                digest.update(sha(bounded(path, 16_777_216)).bytes().toByteArray());
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    static Sha256Digest digest(Map<String, String> values) {
        var canonical = new StringBuilder();
        new TreeMap<>(values).forEach((key, value) -> canonical
                .append(key.length())
                .append(':')
                .append(key)
                .append(value.length())
                .append(':')
                .append(value));
        return sha(canonical.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static Sha256Digest parse(String value) {
        return Sha256Digest.copyOf(HexFormat.of().parseHex(value));
    }

    private static Sha256Digest sha(byte[] bytes) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes));
    }

    private static byte[] bounded(Path path, int maximum) throws IOException {
        if (Files.size(path) > maximum) {
            throw new IllegalArgumentException("Native M4 evidence exceeds its bound");
        }
        return Files.readAllBytes(path);
    }

    private static String required(Properties values, String key) {
        String value = values.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Native M4 evidence omits " + key);
        }
        return value;
    }

    private static void require(Properties values, String key, String expected) {
        if (!required(values, key).equals(expected)) {
            throw new IllegalArgumentException("Native M4 evidence differs: " + key);
        }
    }

    private static int count(Properties values, String key, int maximum) {
        int count = Integer.parseInt(required(values, key));
        if (count <= 0 || count > maximum) {
            throw new IllegalArgumentException("Native M4 closure count is invalid");
        }
        return count;
    }
}
