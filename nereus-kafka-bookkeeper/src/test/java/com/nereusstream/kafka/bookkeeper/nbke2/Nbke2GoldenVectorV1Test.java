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

package com.nereusstream.kafka.bookkeeper.nbke2;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class Nbke2GoldenVectorV1Test {
    private static final Path GOLDENS = Path.of("..", "docs", "v2", "wire", "nbke2-v1-goldens.tsv");

    @Test
    void minimumRepresentativeAndMaximumVectorsRemainImmutable() throws Exception {
        Map<String, Vector> vectors = readVectors();
        assertThat(vectors).hasSize(15);

        verify("minimum", Nbke2GoldenVectorEmitter.minimumFrames(), vectors, true);
        verify(
                "representative",
                List.of(
                        Nbke2TestFrames.runHeader(),
                        Nbke2TestFrames.data(),
                        Nbke2TestFrames.rangeIndexBlock(),
                        Nbke2TestFrames.protocolCheckpoint(),
                        Nbke2TestFrames.runFooter()),
                vectors,
                true);
        verify("maximum", Nbke2GoldenVectorEmitter.maximumFrames(), vectors, false);
    }

    private static void verify(
            String vectorClass, List<Nbke2FrameV1> frames, Map<String, Vector> vectors, boolean containsHex) {
        for (int index = 0; index < frames.size(); index++) {
            Nbke2FrameV1 frame = frames.get(index);
            Vector vector = vectors.get(vectorClass + "/" + frame.frameType().name());
            byte[] encoded = Nbke2CodecV1.encode(Nbke2TestFrames.LEDGER_ID, vector.entryId(), frame);
            assertThat(vector).isNotNull();
            assertThat(vector.length()).isEqualTo(encoded.length);
            assertThat(vector.sha256())
                    .isEqualTo(Sha256Digest.hash(CanonicalBytes.copyOf(encoded)).toHex());
            if (containsHex) {
                assertThat(vector.hex()).isEqualTo(HexFormat.of().formatHex(encoded));
                assertThat(Nbke2CodecV1.decode(
                                HexFormat.of().parseHex(vector.hex()), Nbke2TestFrames.LEDGER_ID, vector.entryId()))
                        .isEqualTo(frame);
            } else {
                assertThat(vector.hex()).isEqualTo("-");
            }
        }
    }

    private static Map<String, Vector> readVectors() throws Exception {
        List<String> lines = Files.readAllLines(GOLDENS);
        assertThat(lines.get(0)).isEqualTo("class\tframeType\tentryId\tlength\tsha256\thex");
        Map<String, Vector> vectors = new HashMap<>();
        for (String line : new ArrayList<>(lines.subList(1, lines.size()))) {
            String[] fields = line.split("\t", -1);
            assertThat(fields).hasSize(6);
            Vector vector = new Vector(Integer.parseInt(fields[2]), Integer.parseInt(fields[3]), fields[4], fields[5]);
            assertThat(vectors.put(fields[0] + "/" + fields[1], vector)).isNull();
        }
        return vectors;
    }

    private record Vector(int entryId, int length, String sha256, String hex) {}
}
