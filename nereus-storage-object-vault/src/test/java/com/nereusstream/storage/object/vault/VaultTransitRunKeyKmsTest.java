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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class VaultTransitRunKeyKmsTest {
    private static final String TOKEN = "unit-test-token";
    private static final String METADATA = "{\"data\":{\"type\":\"aes256-gcm96\",\"latest_version\":1,"
            + "\"min_decryption_version\":0,\"supports_encryption\":true,"
            + "\"supports_decryption\":true,\"derived\":false}}";

    @Test
    void wrapAndUnwrapUseExactNonDerivedRequestsWithoutHiddenContext() throws Exception {
        byte[] plaintext = new byte[32];
        Arrays.fill(plaintext, (byte) 0x5a);
        String encodedPlaintext = java.util.Base64.getEncoder().encodeToString(plaintext);
        String ciphertext = "vault:v1:QUJDRA==";
        String decryptResponse = "{\"data\":{\"plaintext\":\"" + encodedPlaintext + "\"}}";
        try (ScriptedHttpServer server = new ScriptedHttpServer(List.of(
                Response.ok(METADATA),
                Response.ok("{\"data\":{\"ciphertext\":\"" + ciphertext + "\",\"key_version\":1}}"),
                Response.ok(METADATA),
                Response.ok(decryptResponse)))) {
            try (VaultTransitRunKeyKms kms = kms(server.endpoint(), () -> TOKEN)) {
                WrappedRunKeyEnvelope envelope = kms.wrapChecked(plaintext);
                assertThat(envelope.wrappingAlgorithmId()).isEqualTo("AES256_GCM96");
                assertThat(envelope.wrappingKeyVersion()).isEqualTo("1");
                try (ZeroizableRunKey unwrapped = kms.unwrapZeroizable(envelope)) {
                    boolean matches = unwrapped.use(value -> MessageDigest.isEqual(value, plaintext));
                    assertThat(matches).isTrue();
                }
            }
            server.awaitComplete();
            assertThat(server.requests())
                    .extracting(Request::path)
                    .containsExactly(
                            "/v1/transit/keys/nereus-m3-run-key",
                            "/v1/transit/encrypt/nereus-m3-run-key",
                            "/v1/transit/keys/nereus-m3-run-key",
                            "/v1/transit/decrypt/nereus-m3-run-key");
            assertThat(server.requests()).allSatisfy(request -> {
                assertThat(request.token()).isEqualTo(TOKEN);
                assertThat(request.body()).doesNotContain("context");
            });
            assertThat(server.requests().get(1).body())
                    .isEqualTo("{\"plaintext\":\"" + encodedPlaintext + "\",\"key_version\":1}");
            assertThat(server.requests().get(3).body()).isEqualTo("{\"ciphertext\":\"" + ciphertext + "\"}");
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    @Test
    void classifiesAuthorizationUnavailableAndTransientHttpFailuresExactly() throws Exception {
        Map<Integer, VaultTransitException.Kind> cases = Map.of(
                401, VaultTransitException.Kind.AUTHORIZATION,
                403, VaultTransitException.Kind.AUTHORIZATION,
                404, VaultTransitException.Kind.KEY_UNAVAILABLE,
                408, VaultTransitException.Kind.RETRYABLE,
                429, VaultTransitException.Kind.RETRYABLE,
                500, VaultTransitException.Kind.RETRYABLE);
        for (Map.Entry<Integer, VaultTransitException.Kind> testCase : cases.entrySet()) {
            try (ScriptedHttpServer server =
                            new ScriptedHttpServer(List.of(new Response(testCase.getKey(), "{\"errors\":[]}")));
                    VaultTransitRunKeyKms kms = kms(server.endpoint(), () -> TOKEN)) {
                assertThatThrownBy(() -> kms.wrapChecked(new byte[32]))
                        .isInstanceOf(VaultTransitException.class)
                        .satisfies(failure -> assertThat(((VaultTransitException) failure).kind())
                                .isEqualTo(testCase.getValue()));
                server.awaitComplete();
            }
        }
    }

    @Test
    void rejectsDuplicateMetadataFieldsAndDerivedKeys() throws Exception {
        String duplicate = METADATA.replace("\"latest_version\":1", "\"latest_version\":1,\"latest_version\":1");
        assertMetadataFailure(duplicate, VaultTransitException.Kind.MALFORMED_RESPONSE);
        assertMetadataFailure(
                METADATA.replace("\"derived\":false", "\"derived\":true"), VaultTransitException.Kind.KEY_UNAVAILABLE);
    }

    @Test
    void rejectsNonOriginEndpointsAndNonVisibleTokensBeforeDispatch() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        assertThatThrownBy(() -> new VaultTransitRunKeyKms(
                        client,
                        URI.create("http://user@127.0.0.1:8200/path"),
                        "transit",
                        "key",
                        () -> TOKEN,
                        Duration.ofSeconds(1),
                        new SecureRandom()))
                .isInstanceOf(IllegalArgumentException.class);
        try (VaultTransitRunKeyKms kms = kms(URI.create("http://127.0.0.1:1"), () -> "bad\ntoken")) {
            assertThatThrownBy(() -> kms.wrapChecked(new byte[32]))
                    .isInstanceOf(VaultTransitException.class)
                    .satisfies(failure -> assertThat(((VaultTransitException) failure).kind())
                            .isEqualTo(VaultTransitException.Kind.AUTHORIZATION));
        }
    }

    private static void assertMetadataFailure(String metadata, VaultTransitException.Kind expected) throws Exception {
        try (ScriptedHttpServer server = new ScriptedHttpServer(List.of(Response.ok(metadata)));
                VaultTransitRunKeyKms kms = kms(server.endpoint(), () -> TOKEN)) {
            assertThatThrownBy(() -> kms.wrapChecked(new byte[32]))
                    .isInstanceOf(VaultTransitException.class)
                    .satisfies(failure ->
                            assertThat(((VaultTransitException) failure).kind()).isEqualTo(expected));
            server.awaitComplete();
        }
    }

    private static VaultTransitRunKeyKms kms(URI endpoint, java.util.function.Supplier<String> tokenSupplier) {
        return new VaultTransitRunKeyKms(
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(2))
                        .version(HttpClient.Version.HTTP_1_1)
                        .build(),
                endpoint,
                "transit",
                "nereus-m3-run-key",
                tokenSupplier,
                Duration.ofSeconds(2),
                new SecureRandom());
    }

    private record Response(int status, String body) {
        private static Response ok(String body) {
            return new Response(200, body);
        }
    }

    private record Request(String path, String token, String body) {}

    private static final class ScriptedHttpServer implements AutoCloseable {
        private static final byte[] HEADER_TERMINATOR = {'\r', '\n', '\r', '\n'};
        private final ServerSocket server;
        private final List<Response> responses;
        private final List<Request> requests = new ArrayList<>();
        private final Thread thread;
        private volatile Throwable failure;

        private ScriptedHttpServer(List<Response> responses) throws IOException {
            this.responses = List.copyOf(responses);
            server = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
            thread = new Thread(this::serve, "vault-transit-unit-http");
            thread.setDaemon(true);
            thread.start();
        }

        private URI endpoint() {
            return URI.create("http://127.0.0.1:" + server.getLocalPort());
        }

        private synchronized List<Request> requests() {
            return List.copyOf(requests);
        }

        private void serve() {
            try {
                for (Response response : responses) {
                    try (Socket socket = server.accept()) {
                        socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(5));
                        Request request = readRequest(socket);
                        synchronized (this) {
                            requests.add(request);
                        }
                        byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
                        String head = "HTTP/1.1 " + response.status() + " " + reason(response.status()) + "\r\n"
                                + "Content-Type: application/json\r\n"
                                + "Content-Length: " + body.length + "\r\n"
                                + "Connection: close\r\n\r\n";
                        socket.getOutputStream().write(head.getBytes(StandardCharsets.US_ASCII));
                        socket.getOutputStream().write(body);
                        socket.getOutputStream().flush();
                    }
                }
            } catch (Throwable caught) {
                if (!server.isClosed()) {
                    failure = caught;
                }
            }
        }

        private static Request readRequest(Socket socket) throws IOException {
            BufferedInputStream input = new BufferedInputStream(socket.getInputStream());
            ByteArrayOutputStream header = new ByteArrayOutputStream();
            int matched = 0;
            while (matched < HEADER_TERMINATOR.length) {
                int value = input.read();
                if (value < 0 || header.size() >= 64 * 1024) {
                    throw new IOException("bounded unit HTTP request header is incomplete");
                }
                header.write(value);
                matched = value == HEADER_TERMINATOR[matched] ? matched + 1 : (value == HEADER_TERMINATOR[0] ? 1 : 0);
            }
            String[] lines = header.toString(StandardCharsets.US_ASCII).split("\\r\\n");
            String[] requestLine = lines[0].split(" ", 3);
            if (requestLine.length != 3) {
                throw new IOException("unit HTTP request line is malformed");
            }
            int contentLength = 0;
            String token = null;
            for (int index = 1; index < lines.length; index++) {
                int separator = lines[index].indexOf(':');
                if (separator < 0) {
                    continue;
                }
                String name = lines[index].substring(0, separator).trim().toLowerCase(Locale.ROOT);
                String value = lines[index].substring(separator + 1).trim();
                if ("content-length".equals(name)) {
                    contentLength = Integer.parseInt(value);
                } else if ("x-vault-token".equals(name)) {
                    token = value;
                }
            }
            if (contentLength < 0 || contentLength > 64 * 1024) {
                throw new IOException("unit HTTP request body exceeds its cap");
            }
            byte[] body = input.readNBytes(contentLength);
            if (body.length != contentLength) {
                throw new IOException("unit HTTP request body is incomplete");
            }
            return new Request(requestLine[1], token, new String(body, StandardCharsets.UTF_8));
        }

        private static String reason(int status) {
            return switch (status) {
                case 200 -> "OK";
                case 401 -> "Unauthorized";
                case 403 -> "Forbidden";
                case 404 -> "Not Found";
                case 408 -> "Request Timeout";
                case 429 -> "Too Many Requests";
                default -> "Server Error";
            };
        }

        private void awaitComplete() throws Exception {
            thread.join(TimeUnit.SECONDS.toMillis(10));
            if (thread.isAlive()) {
                throw new IllegalStateException("unit HTTP server did not complete within its bound");
            }
            if (failure != null) {
                throw new IllegalStateException("unit HTTP server failed", failure);
            }
            assertThat(requests()).hasSize(responses.size());
        }

        @Override
        public void close() throws Exception {
            server.close();
            thread.join(TimeUnit.SECONDS.toMillis(2));
        }
    }
}
