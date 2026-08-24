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

package com.nereusstream.storage.object.s3;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Exact-target temporary Docker CLI container for evidence environments where docker-java cannot discover Desktop. */
final class DockerCliEvidenceContainer implements AutoCloseable {
    private final String containerId;
    private final URI endpoint;
    private final String imageConfigDigest;

    private DockerCliEvidenceContainer(String containerId, URI endpoint, String imageConfigDigest) {
        this.containerId = containerId;
        this.endpoint = endpoint;
        this.imageConfigDigest = imageConfigDigest;
    }

    static DockerCliEvidenceContainer start(
            String evidenceLabel, String image, int containerPort, List<String> environment, List<String> command)
            throws Exception {
        if (!image.matches("[^\\s@]+@sha256:[0-9a-f]{64}")) {
            throw new IllegalArgumentException("evidence image must be one exact lowercase manifest digest reference");
        }
        List<String> arguments = new ArrayList<>();
        arguments.addAll(List.of(
                "docker",
                "run",
                "--rm",
                "--detach",
                "--publish",
                "127.0.0.1::" + containerPort,
                "--label",
                "com.nereusstream.evidence=" + evidenceLabel));
        environment.forEach(value -> arguments.addAll(List.of("--env", value)));
        arguments.add(image);
        arguments.addAll(command);
        String containerId = exactContainerId(execute(arguments, Duration.ofMinutes(5)));
        if (containerId == null) {
            throw new IllegalStateException("docker run did not return one exact container ID");
        }
        try {
            String published =
                    execute(List.of("docker", "port", containerId, containerPort + "/tcp"), Duration.ofSeconds(30));
            if (!published.matches("127\\.0\\.0\\.1:[1-9][0-9]{0,4}")) {
                throw new IllegalStateException("docker port did not return one loopback port");
            }
            int separator = published.lastIndexOf(':');
            int hostPort = Integer.parseInt(published.substring(separator + 1));
            if (hostPort > 65_535) {
                throw new IllegalStateException("docker port returned a value outside the TCP port domain");
            }
            String repoDigests = execute(
                    List.of(
                            "docker",
                            "image",
                            "inspect",
                            "--format",
                            "{{range .RepoDigests}}{{println .}}{{end}}",
                            image),
                    Duration.ofSeconds(30));
            if (repoDigests.lines().filter(image::equals).count() != 1) {
                throw new IllegalStateException("local image inventory lacks the exact manifest digest reference");
            }
            String imageConfigDigest = execute(
                    List.of("docker", "inspect", "--format", "{{.Image}}", containerId), Duration.ofSeconds(30));
            if (!imageConfigDigest.matches("sha256:[0-9a-f]{64}")) {
                throw new IllegalStateException("docker inspect did not return one exact image config digest");
            }
            return new DockerCliEvidenceContainer(
                    containerId, URI.create("http://127.0.0.1:" + hostPort), imageConfigDigest);
        } catch (Throwable failure) {
            stop(containerId);
            throw failure;
        }
    }

    String containerId() {
        return containerId;
    }

    URI endpoint() {
        return endpoint;
    }

    String imageConfigDigest() {
        return imageConfigDigest;
    }

    @Override
    public void close() throws Exception {
        stop(containerId);
    }

    private static void stop(String containerId) throws Exception {
        execute(List.of("docker", "stop", "--timeout", "5", containerId), Duration.ofSeconds(30));
        for (int attempt = 0; attempt < 50; attempt++) {
            if (containerIsAbsent(containerId)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("evidence container was not auto-removed after bounded stop");
    }

    private static boolean containerIsAbsent(String containerId) throws Exception {
        Process process = new ProcessBuilder("docker", "container", "inspect", containerId)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("bounded docker container inspection timed out");
        }
        return process.exitValue() != 0;
    }

    private static String exactContainerId(String output) {
        String found = null;
        for (String line : output.split("\\R")) {
            String candidate = line.trim();
            if (candidate.matches("[0-9a-f]{64}")) {
                if (found != null) {
                    return null;
                }
                found = candidate;
            }
        }
        return found;
    }

    private static String execute(List<String> arguments, Duration timeout) throws Exception {
        Process process =
                new ProcessBuilder(arguments).redirectErrorStream(true).start();
        CompletableFuture<byte[]> output = CompletableFuture.supplyAsync(() -> {
            try {
                return process.getInputStream().readAllBytes();
            } catch (java.io.IOException failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
        });
        if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("bounded docker command timed out");
        }
        byte[] bytes = output.get(30, TimeUnit.SECONDS);
        if (bytes.length > 64 * 1024 || process.exitValue() != 0) {
            throw new IllegalStateException("bounded docker command failed without exposing command output");
        }
        return new String(bytes, StandardCharsets.UTF_8).trim();
    }
}
