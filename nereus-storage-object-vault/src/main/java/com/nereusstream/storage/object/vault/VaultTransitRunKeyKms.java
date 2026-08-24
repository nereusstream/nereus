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

package com.nereusstream.storage.object.vault;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.control.WalRunTerminalClosureProofV1;
import com.nereusstream.storage.object.kms.KmsTransport;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Vault Transit AES-256 run-key wrap/unwrap bound to one immutable key identity and explicit key version.
 * Application-owned plaintext arrays are erased; token Strings and JDK HTTP/TLS internal buffers are explicitly
 * outside the provable zeroization boundary.
 */
public final class VaultTransitRunKeyKms implements KmsTransport, AutoCloseable {
    public static final String ADAPTER_VERSION = "nereus-vault-transit-v1/jdk-http";
    public static final String PROVIDER_ID = "HASHICORP_VAULT_TRANSIT";
    public static final String WRAPPING_ALGORITHM_ID = "AES256_GCM96";
    private static final int MAXIMUM_RESPONSE_BYTES = 64 * 1024;
    private static final Pattern PATH_COMPONENT = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]{0,127}");
    private static final Pattern VAULT_CIPHERTEXT = Pattern.compile("vault:v([1-9][0-9]*):[A-Za-z0-9+/]+={0,2}");

    private final HttpClient client;
    private final URI endpoint;
    private final String mount;
    private final String keyName;
    private final String keyIdentity;
    private final Supplier<String> tokenSupplier;
    private final Duration timeout;
    private final SecureRandom random;
    private final AtomicBoolean closed = new AtomicBoolean();

    public VaultTransitRunKeyKms(
            HttpClient client,
            URI endpoint,
            String mount,
            String keyName,
            Supplier<String> tokenSupplier,
            Duration timeout,
            SecureRandom random) {
        this.client = Objects.requireNonNull(client, "client");
        this.endpoint = normalizeEndpoint(endpoint);
        this.mount = requirePathComponent(mount, "mount");
        this.keyName = requirePathComponent(keyName, "keyName");
        this.keyIdentity = "vault-transit://" + this.mount + "/keys/" + this.keyName;
        this.tokenSupplier = Objects.requireNonNull(tokenSupplier, "tokenSupplier");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.random = Objects.requireNonNull(random, "random");
        if (timeout.isZero() || timeout.isNegative() || timeout.compareTo(Duration.ofMinutes(2)) > 0) {
            throw new IllegalArgumentException("Vault request timeout must be in (0,2m]");
        }
    }

    public GeneratedRunKey generateAndWrap() throws IOException, InterruptedException {
        requireOpen();
        byte[] plaintext = new byte[32];
        random.nextBytes(plaintext);
        try {
            WrappedRunKeyEnvelope envelope = wrapChecked(plaintext);
            return new GeneratedRunKey(new ZeroizableRunKey(plaintext), envelope);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    public WrappedRunKeyEnvelope wrapChecked(byte[] plaintextRunKey) throws IOException, InterruptedException {
        requireOpen();
        Objects.requireNonNull(plaintextRunKey, "plaintextRunKey");
        if (plaintextRunKey.length != 32) {
            throw new IllegalArgumentException("WalRun key must be exactly 32 bytes");
        }
        KeyMetadata metadata = readKeyMetadata();
        byte[] copy = plaintextRunKey.clone();
        byte[] encoded = null;
        byte[] request = null;
        byte[] response = null;
        try {
            encoded = Base64.getEncoder().encode(copy);
            request =
                    asciiRequest("{\"plaintext\":\"", encoded, "\",\"key_version\":" + metadata.latestVersion() + "}");
            response = request("POST", transitPath("encrypt"), request);
            String ciphertext = JsonFields.requiredString(response, "ciphertext");
            int responseVersion = JsonFields.requiredPositiveInt(response, "key_version");
            int ciphertextVersion = ciphertextVersion(ciphertext);
            if (responseVersion != metadata.latestVersion() || ciphertextVersion != responseVersion) {
                throw new VaultTransitException(
                        VaultTransitException.Kind.VERSION_MISMATCH,
                        "Vault Transit encryption response did not bind the requested key version");
            }
            return new WrappedRunKeyEnvelope(
                    PROVIDER_ID,
                    WRAPPING_ALGORITHM_ID,
                    keyIdentity,
                    Integer.toString(responseVersion),
                    CanonicalBytes.copyOf(ciphertext.getBytes(StandardCharsets.UTF_8)));
        } finally {
            Arrays.fill(copy, (byte) 0);
            if (encoded != null) {
                Arrays.fill(encoded, (byte) 0);
            }
            if (request != null) {
                Arrays.fill(request, (byte) 0);
            }
            if (response != null) {
                Arrays.fill(response, (byte) 0);
            }
        }
    }

    public ZeroizableRunKey unwrapZeroizable(WrappedRunKeyEnvelope envelope) throws IOException, InterruptedException {
        requireOpen();
        Objects.requireNonNull(envelope, "envelope");
        if (!PROVIDER_ID.equals(envelope.providerId())
                || !WRAPPING_ALGORITHM_ID.equals(envelope.wrappingAlgorithmId())
                || !keyIdentity.equals(envelope.wrappingKeyId())) {
            throw new VaultTransitException(
                    VaultTransitException.Kind.VERSION_MISMATCH,
                    "wrapped key provider, algorithm, or identity differs from this Vault key");
        }
        int envelopeVersion = parsePositiveVersion(envelope.wrappingKeyVersion());
        byte[] ciphertextBytes = envelope.wrappedKey().toByteArray();
        String ciphertext;
        try {
            ciphertext = strictUtf8(ciphertextBytes, "wrapped Vault ciphertext");
        } finally {
            Arrays.fill(ciphertextBytes, (byte) 0);
        }
        if (ciphertextVersion(ciphertext) != envelopeVersion) {
            throw new VaultTransitException(
                    VaultTransitException.Kind.VERSION_MISMATCH,
                    "wrapped key version differs from the Vault ciphertext version");
        }
        KeyMetadata metadata = readKeyMetadata();
        if (envelopeVersion < metadata.minimumDecryptionVersion() || envelopeVersion > metadata.latestVersion()) {
            throw new VaultTransitException(
                    VaultTransitException.Kind.KEY_UNAVAILABLE,
                    "wrapped key version is outside the Vault decryption window");
        }
        byte[] request = ("{\"ciphertext\":\"" + ciphertext + "\"}").getBytes(StandardCharsets.US_ASCII);
        byte[] response = null;
        byte[] encodedPlaintext = null;
        byte[] decoded = null;
        try {
            try {
                response = request("POST", transitPath("decrypt"), request);
                encodedPlaintext = JsonFields.requiredUnescapedAsciiBytes(response, "plaintext");
                decoded = Base64.getDecoder().decode(encodedPlaintext);
            } catch (IllegalArgumentException failure) {
                throw new VaultTransitException(
                        VaultTransitException.Kind.MALFORMED_RESPONSE,
                        "Vault Transit returned malformed plaintext encoding",
                        failure);
            }
            if (decoded.length != 32) {
                throw new VaultTransitException(
                        VaultTransitException.Kind.MALFORMED_RESPONSE,
                        "Vault Transit returned a non-32-byte WalRun key");
            }
            return new ZeroizableRunKey(decoded);
        } finally {
            Arrays.fill(request, (byte) 0);
            if (response != null) {
                Arrays.fill(response, (byte) 0);
            }
            if (encodedPlaintext != null) {
                Arrays.fill(encodedPlaintext, (byte) 0);
            }
            if (decoded != null) {
                Arrays.fill(decoded, (byte) 0);
            }
        }
    }

    @Override
    public WrappedRunKeyEnvelope wrap(String requestedKeyIdentity, byte[] plaintextRunKey) {
        if (!keyIdentity.equals(requestedKeyIdentity)) {
            throw new IllegalArgumentException("requested KMS key identity differs from this Vault transport");
        }
        try {
            return wrapChecked(plaintextRunKey);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new VaultKmsOperationException(
                    VaultTransitException.Kind.RETRYABLE, "Vault Transit wrap was interrupted", failure);
        } catch (VaultTransitException failure) {
            throw new VaultKmsOperationException(failure.kind(), "Vault Transit wrap failed", failure);
        } catch (IOException failure) {
            throw new VaultKmsOperationException(
                    VaultTransitException.Kind.RETRYABLE, "Vault Transit wrap failed", failure);
        }
    }

    @Override
    public byte[] unwrap(WrappedRunKeyEnvelope envelope) {
        try (ZeroizableRunKey key = unwrapZeroizable(envelope)) {
            return key.use(bytes -> bytes.clone());
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new VaultKmsOperationException(
                    VaultTransitException.Kind.RETRYABLE, "Vault Transit unwrap was interrupted", failure);
        } catch (VaultTransitException failure) {
            throw new VaultKmsOperationException(failure.kind(), "Vault Transit unwrap failed", failure);
        } catch (IOException failure) {
            throw new VaultKmsOperationException(
                    VaultTransitException.Kind.RETRYABLE, "Vault Transit unwrap failed", failure);
        }
    }

    /** Rotation is a successor action gated by a production WalRun terminal-closure proof. */
    public int rotateAfterRunSeal(WalRunTerminalClosureProofV1 closureProof) throws IOException, InterruptedException {
        requireOpen();
        Objects.requireNonNull(closureProof, "closureProof");
        if (closureProof.root().rootSha256().isZero()
                || closureProof.sealSha256().isZero()
                || !closureProof.sealKey().contains("/runs/")) {
            throw new IllegalArgumentException("WalRun terminal closure proof is invalid");
        }
        int before = readKeyMetadata().latestVersion();
        String rotatePath = "/v1/" + mount + "/keys/" + keyName + "/rotate";
        byte[] rotateResponse = request("POST", rotatePath, "{}".getBytes(StandardCharsets.US_ASCII));
        Arrays.fill(rotateResponse, (byte) 0);
        int after = readKeyMetadata().latestVersion();
        if (after <= before) {
            throw new VaultTransitException(
                    VaultTransitException.Kind.VERSION_MISMATCH, "Vault Transit rotation did not advance key version");
        }
        return after;
    }

    public String keyIdentity() {
        return keyIdentity;
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private KeyMetadata readKeyMetadata() throws IOException, InterruptedException {
        byte[] response = request("GET", "/v1/" + mount + "/keys/" + keyName, null);
        try {
            String type = JsonFields.requiredString(response, "type");
            int latest = JsonFields.requiredPositiveInt(response, "latest_version");
            int minimum = JsonFields.requiredNonNegativeInt(response, "min_decryption_version");
            boolean encryption = JsonFields.requiredBoolean(response, "supports_encryption");
            boolean decryption = JsonFields.requiredBoolean(response, "supports_decryption");
            boolean derived = JsonFields.requiredBoolean(response, "derived");
            if (!"aes256-gcm96".equals(type) || !encryption || !decryption || derived || minimum > latest) {
                throw new VaultTransitException(
                        VaultTransitException.Kind.KEY_UNAVAILABLE,
                        "Vault Transit key does not satisfy AES-256-GCM wrap/decrypt admission");
            }
            return new KeyMetadata(latest, minimum);
        } finally {
            Arrays.fill(response, (byte) 0);
        }
    }

    private byte[] request(String method, String path, byte[] json) throws IOException, InterruptedException {
        requireOpen();
        String token = tokenSupplier.get();
        if (token == null || token.isBlank() || token.length() > 4_096 || !isVisibleAscii(token)) {
            throw new VaultTransitException(
                    VaultTransitException.Kind.AUTHORIZATION, "Vault token supplier returned no usable credential");
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint.resolve(path))
                .timeout(timeout)
                .header("X-Vault-Token", token)
                .header("Accept", "application/json");
        if (json == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofByteArray(json));
        }
        HttpResponse<InputStream> response;
        try {
            response = client.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (java.net.http.HttpTimeoutException failure) {
            throw new VaultTransitException(
                    VaultTransitException.Kind.RETRYABLE, "Vault Transit request timed out", failure);
        } catch (IOException failure) {
            throw new VaultTransitException(
                    VaultTransitException.Kind.RETRYABLE, "Vault Transit transport did not complete", failure);
        }
        try (InputStream body = response.body()) {
            byte[] bytes = readBounded(body);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return bytes;
            }
            try {
                if (response.statusCode() == 401 || response.statusCode() == 403) {
                    throw new VaultTransitException(
                            VaultTransitException.Kind.AUTHORIZATION, "Vault Transit rejected authorization");
                }
                if (response.statusCode() == 404) {
                    throw new VaultTransitException(
                            VaultTransitException.Kind.KEY_UNAVAILABLE, "Vault Transit key or mount is unavailable");
                }
                if (response.statusCode() == 408 || response.statusCode() == 429 || response.statusCode() >= 500) {
                    throw new VaultTransitException(
                            VaultTransitException.Kind.RETRYABLE, "Vault Transit request failed transiently");
                }
                throw new VaultTransitException(
                        VaultTransitException.Kind.KEY_UNAVAILABLE, "Vault Transit rejected the key operation");
            } finally {
                Arrays.fill(bytes, (byte) 0);
            }
        }
    }

    private static byte[] readBounded(InputStream input) throws IOException {
        byte[] bounded = new byte[MAXIMUM_RESPONSE_BYTES + 1];
        int count = 0;
        try {
            while (true) {
                int read = input.read(bounded, count, bounded.length - count);
                if (read < 0) {
                    return Arrays.copyOf(bounded, count);
                }
                if (read == 0) {
                    throw new VaultTransitException(
                            VaultTransitException.Kind.MALFORMED_RESPONSE,
                            "Vault Transit response stream made no bounded progress");
                }
                count = Math.addExact(count, read);
                if (count > MAXIMUM_RESPONSE_BYTES) {
                    throw new VaultTransitException(
                            VaultTransitException.Kind.MALFORMED_RESPONSE,
                            "Vault Transit response exceeded the fixed parser cap");
                }
            }
        } finally {
            Arrays.fill(bounded, (byte) 0);
        }
    }

    private static byte[] asciiRequest(String prefix, byte[] middle, String suffix) {
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.US_ASCII);
        byte[] suffixBytes = suffix.getBytes(StandardCharsets.US_ASCII);
        byte[] result = new byte[Math.addExact(Math.addExact(prefixBytes.length, middle.length), suffixBytes.length)];
        System.arraycopy(prefixBytes, 0, result, 0, prefixBytes.length);
        System.arraycopy(middle, 0, result, prefixBytes.length, middle.length);
        System.arraycopy(suffixBytes, 0, result, prefixBytes.length + middle.length, suffixBytes.length);
        return result;
    }

    private String transitPath(String operation) {
        return "/v1/" + mount + "/" + operation + "/" + keyName;
    }

    private void requireOpen() throws VaultTransitException {
        if (closed.get()) {
            throw new VaultTransitException(VaultTransitException.Kind.CLOSED, "Vault Transit KMS is closed");
        }
    }

    private static URI normalizeEndpoint(URI endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        if (!"http".equals(endpoint.getScheme()) && !"https".equals(endpoint.getScheme())) {
            throw new IllegalArgumentException("Vault endpoint must use HTTP or HTTPS");
        }
        if (endpoint.getHost() == null
                || endpoint.getRawUserInfo() != null
                || (endpoint.getRawPath() != null
                        && !endpoint.getRawPath().isEmpty()
                        && !"/".equals(endpoint.getRawPath()))
                || endpoint.getRawQuery() != null
                || endpoint.getRawFragment() != null) {
            throw new IllegalArgumentException("Vault endpoint must be an absolute origin URI");
        }
        String value = endpoint.toString();
        return URI.create(value.endsWith("/") ? value : value + "/");
    }

    private static String requirePathComponent(String value, String label) {
        Objects.requireNonNull(value, label);
        if (!PATH_COMPONENT.matcher(value).matches()) {
            throw new IllegalArgumentException(label + " is not a canonical Vault path component");
        }
        return value;
    }

    private static int parsePositiveVersion(String value) throws VaultTransitException {
        try {
            int version = Integer.parseInt(value);
            if (version <= 0 || !Integer.toString(version).equals(value)) {
                throw new NumberFormatException("not canonical positive decimal");
            }
            return version;
        } catch (NumberFormatException failure) {
            throw new VaultTransitException(
                    VaultTransitException.Kind.VERSION_MISMATCH,
                    "wrapped key version is not canonical positive decimal",
                    failure);
        }
    }

    private static int ciphertextVersion(String ciphertext) throws VaultTransitException {
        Matcher matcher = VAULT_CIPHERTEXT.matcher(ciphertext);
        if (!matcher.matches()) {
            throw new VaultTransitException(
                    VaultTransitException.Kind.MALFORMED_RESPONSE,
                    "Vault Transit ciphertext is not in the versioned envelope format");
        }
        return parsePositiveVersion(matcher.group(1));
    }

    private static String strictUtf8(byte[] bytes, String label) throws VaultTransitException {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw new VaultTransitException(
                    VaultTransitException.Kind.MALFORMED_RESPONSE, label + " is not canonical UTF-8", failure);
        }
    }

    private static boolean isVisibleAscii(String value) {
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character < 0x21 || character > 0x7e) {
                return false;
            }
        }
        return true;
    }

    public record GeneratedRunKey(ZeroizableRunKey plaintextKey, WrappedRunKeyEnvelope envelope)
            implements AutoCloseable {
        public GeneratedRunKey {
            Objects.requireNonNull(plaintextKey, "plaintextKey");
            Objects.requireNonNull(envelope, "envelope");
        }

        @Override
        public void close() {
            plaintextKey.close();
        }
    }

    private record KeyMetadata(int latestVersion, int minimumDecryptionVersion) {}

    /** Bounded field extractor for Vault's trusted JSON response; it never logs or retains a response tree. */
    private static final class JsonFields {
        private static byte[] requiredUnescapedAsciiBytes(byte[] json, String field) throws VaultTransitException {
            byte[] marker = ("\"" + field + "\"").getBytes(StandardCharsets.US_ASCII);
            int markerStart = indexOf(json, marker, 0);
            if (markerStart < 0 || indexOf(json, marker, markerStart + marker.length) >= 0) {
                throw malformed(field);
            }
            int cursor = markerStart + marker.length;
            while (cursor < json.length && isWhitespace(json[cursor])) {
                cursor++;
            }
            if (cursor >= json.length || json[cursor++] != ':') {
                throw malformed(field);
            }
            while (cursor < json.length && isWhitespace(json[cursor])) {
                cursor++;
            }
            if (cursor >= json.length || json[cursor++] != '"') {
                throw malformed(field);
            }
            int end = cursor;
            while (end < json.length && json[end] != '"') {
                int value = Byte.toUnsignedInt(json[end]);
                if (value == '\\' || value < 0x20 || value > 0x7e) {
                    throw malformed(field);
                }
                end++;
            }
            if (end == json.length) {
                throw malformed(field);
            }
            return Arrays.copyOfRange(json, cursor, end);
        }

        private static int indexOf(byte[] input, byte[] needle, int start) {
            outer:
            for (int index = start; index <= input.length - needle.length; index++) {
                for (int offset = 0; offset < needle.length; offset++) {
                    if (input[index + offset] != needle[offset]) {
                        continue outer;
                    }
                }
                return index;
            }
            return -1;
        }

        private static boolean isWhitespace(byte value) {
            return value == ' ' || value == '\n' || value == '\r' || value == '\t';
        }

        private static String requiredString(byte[] json, String field) throws VaultTransitException {
            String input = strictUtf8(json, "Vault Transit JSON response");
            Matcher matcher = Pattern.compile("\\\"" + Pattern.quote(field) + "\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"")
                    .matcher(input);
            if (!matcher.find() || matcher.group(1).indexOf('\\') >= 0) {
                throw malformed(field);
            }
            String value = matcher.group(1);
            if (matcher.find()) {
                throw malformed(field);
            }
            return value;
        }

        private static int requiredPositiveInt(byte[] json, String field) throws VaultTransitException {
            int value = requiredNonNegativeInt(json, field);
            if (value <= 0) {
                throw malformed(field);
            }
            return value;
        }

        private static int requiredNonNegativeInt(byte[] json, String field) throws VaultTransitException {
            String input = strictUtf8(json, "Vault Transit JSON response");
            Matcher matcher = Pattern.compile("\\\"" + Pattern.quote(field) + "\\\"\\s*:\\s*([0-9]+)")
                    .matcher(input);
            if (!matcher.find()) {
                throw malformed(field);
            }
            try {
                String encoded = matcher.group(1);
                if (matcher.find()) {
                    throw malformed(field);
                }
                int value = Integer.parseInt(encoded);
                if (value < 0) {
                    throw new NumberFormatException("negative");
                }
                return value;
            } catch (NumberFormatException failure) {
                throw malformed(field);
            }
        }

        private static boolean requiredBoolean(byte[] json, String field) throws VaultTransitException {
            String input = strictUtf8(json, "Vault Transit JSON response");
            Matcher matcher = Pattern.compile("\\\"" + Pattern.quote(field) + "\\\"\\s*:\\s*(true|false)")
                    .matcher(input);
            if (!matcher.find()) {
                throw malformed(field);
            }
            String encoded = matcher.group(1);
            if (matcher.find()) {
                throw malformed(field);
            }
            return Boolean.parseBoolean(encoded);
        }

        private static VaultTransitException malformed(String field) {
            return new VaultTransitException(
                    VaultTransitException.Kind.MALFORMED_RESPONSE,
                    "Vault Transit response lacks a canonical " + field + " field");
        }
    }
}
