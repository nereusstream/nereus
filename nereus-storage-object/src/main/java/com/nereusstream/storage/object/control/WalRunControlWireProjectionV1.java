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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Machine-readable exact field projection for NWR1/NWP1/NWS1/NWC1/NWH1. */
public final class WalRunControlWireProjectionV1 {
    public static final int MAX_CONTROL_RECORD_BYTES = 1_048_576;
    public static final int MAX_CHECKPOINT_PAGE_BYTES = 65_536;
    public static final int MAX_CHECKPOINT_ROWS = 256;

    private static final List<Field> FIELDS = List.of(
            field("ALL", "magic", "0", "4", "u32be", "record-specific ASCII magic"),
            field("ALL", "wireVersion", "4", "1", "u8", "exactly 1"),
            field("ALL", "reserved8", "5", "1", "u8", "exactly 0"),
            field("ALL", "reserved16", "6", "2", "u16be", "exactly 0"),
            field("NWR1", "shardId", "8", "4", "i32be", "non-negative"),
            field("NWR1", "shardRunEpoch", "12", "8", "i64be", "non-negative"),
            field("NWR1", "walRunSessionId.high", "20", "8", "i64be", "Id128 non-zero"),
            field("NWR1", "walRunSessionId.low", "28", "8", "i64be", "Id128 non-zero"),
            field("NWR1", "openedAtMillis", "36", "8", "i64be", "durable non-negative"),
            field("NWR1", "protocolCellLength", "44", "2", "u16be", "1..64"),
            field("NWR1", "exactProtocolCellNpc1", "46", "N", "bytes", "canonical NPC1"),
            field("NWR1", "cellProviderScopeId", "46+N", "32", "bytes", "SHA-256 typed scope"),
            field("NWR1", "formatContract[0..26]", "78+N", "27", "u8[27]", "each exact closed v1 code"),
            field("NWR1", "maxCanonicalBodyBytes", "105+N", "8", "i64be", "1..4GiB"),
            field("NWR1", "maxDirectoryPrefixBytes", "113+N", "4", "i32be", "1..4MiB"),
            field("NWR1", "maxDirectoryPlaintextBytes", "117+N", "4", "i32be", "1..4194032"),
            field("NWR1", "maxBindingContexts", "121+N", "4", "i32be", "1..256"),
            field("NWR1", "maxAppendUnits", "125+N", "4", "i32be", "1..65536"),
            field("NWR1", "maxFrames", "129+N", "4", "i32be", "1..65536"),
            field("NWR1", "maxDecodedFrameBytes", "133+N", "4", "i32be", "1..64MiB"),
            field("NWR1", "maxStoredFrameBytes", "137+N", "4", "i32be", "decoded+16"),
            field("NWR1", "maxDecodedAppendUnitBytes", "141+N", "8", "i64be", "frame..4GiB"),
            field("NWR1", "maxTotalDecodedPayloadBytes", "149+N", "8", "i64be", "appendUnit..4GiB"),
            field("NWR1", "maxExtentCount", "157+N", "8", "i64be", "positive"),
            field("NWR1", "maxRunCanonicalBodyBytes", "165+N", "8", "i64be", "positive"),
            field("NWR1", "maxRunAgeMillis", "173+N", "8", "i64be", "positive"),
            field("NWR1", "maxRecoverablePredecessorRuns", "181+N", "4", "i32be", "non-negative"),
            field("NWR1", "checkpointCadenceMillis", "185+N", "8", "i64be", "non-negative"),
            field("NWR1", "maxUncheckpointedExtents", "193+N", "4", "i32be", "positive"),
            field("NWR1", "maxUncheckpointedBytes", "197+N", "8", "i64be", "positive"),
            field("NWR1", "maxUncheckpointedAgeMillis", "205+N", "8", "i64be", "positive"),
            field("NWR1", "maxRowsPerPage", "213+N", "4", "i32be", "1..256"),
            field("NWR1", "maxCanonicalPageBytes", "217+N", "4", "i32be", "1..65536"),
            field("NWR1", "providerAccessProfile", "221+N", "1", "u8", "C1=1,C2=2"),
            field("NWR1", "adapterVersionLength", "222+N", "2", "u16be", "1..128"),
            field("NWR1", "adapterVersion", "224+N", "A", "canonical UTF-8", "non-empty,no NUL"),
            field("NWR1", "canonicalizerVersionLength", "224+N+A", "2", "u16be", "1..128"),
            field("NWR1", "canonicalizerVersion", "226+N+A", "C", "canonical UTF-8", "non-empty,no NUL"),
            field("NWR1", "exclusivePrefixLength", "226+N+A+C", "2", "u16be", "1..512"),
            field("NWR1", "exclusivePrefix", "228+N+A+C", "P", "ASCII path", "exact relative grammar"),
            field("NWR1", "providerProofMode", "228+N+A+C+P", "1", "u8", "NONE=0,VERSION=1"),
            field("NWR1", "proofTokenHardCap", "229+N+A+C+P", "2", "u16be", "0..65535"),
            field("NWR1", "providerMaxObjectBytes", "231+N+A+C+P", "8", "i64be", "positive"),
            field("NWR1", "providerMaxSinglePutBytes", "239+N+A+C+P", "8", "i64be", ">=object"),
            field("NWR1", "providerMaxSingleRangeBytes", "247+N+A+C+P", "4", "i32be", "positive"),
            field("NWR1", "maxPrefixSegmentsPerExtent", "251+N+A+C+P", "4", "i32be", "C1 exactly 1"),
            field("NWR1", "providerMaxListPageKeys", "255+N+A+C+P", "4", "i32be", "positive"),
            field("NWR1", "providerCapabilityReceiptSha256", "259+N+A+C+P", "32", "bytes", "non-zero"),
            field("NWR1", "maxLiveRoots", "291+N+A+C+P", "4", "i32be", "positive"),
            field("NWR1", "maxPredecessorRuns", "295+N+A+C+P", "4", "i32be", "non-negative"),
            field("NWR1", "maxListPages", "299+N+A+C+P", "4", "i32be", "positive"),
            field("NWR1", "maxListedKeys", "303+N+A+C+P", "8", "i64be", "positive"),
            field("NWR1", "maxListedKeyBytes", "311+N+A+C+P", "8", "i64be", "positive"),
            field("NWR1", "maxHeadRequests", "319+N+A+C+P", "4", "i32be", "C1 exactly 0"),
            field("NWR1", "maxRangeGetRequests", "323+N+A+C+P", "4", "i32be", "positive"),
            field("NWR1", "maxFullGetRequests", "327+N+A+C+P", "4", "i32be", "non-negative"),
            field("NWR1", "maxRecoveryCanonicalBytes", "331+N+A+C+P", "8", "i64be", "positive"),
            field("NWR1", "maxDecodedContexts", "339+N+A+C+P", "8", "i64be", "positive"),
            field("NWR1", "maxDecodedFrames", "347+N+A+C+P", "8", "i64be", "positive"),
            field("NWR1", "maxDecodedCommitSets", "355+N+A+C+P", "8", "i64be", "positive"),
            field("NWR1", "maxWorkingMemoryBytes", "363+N+A+C+P", "8", "i64be", "positive"),
            field("NWR1", "maxRecoveryConcurrency", "371+N+A+C+P", "4", "i32be", "positive"),
            field("NWR1", "maxRetryAttempts", "375+N+A+C+P", "4", "i32be", "non-negative"),
            field("NWR1", "maxRecoveryWallTimeNanos", "379+N+A+C+P", "8", "i64be", "positive"),
            field("NWR1", "wrappedRunKeyEnvelope", "387+N+A+C+P", "E", "NWE1 framed", "33..21660"),
            field("NWR1", "predecessorPresent", "387+N+A+C+P+E", "1", "u8", "0 or 1"),
            field("NWR1", "predecessorRootReference", "388+N+A+C+P+E", "46+R", "reference", "if present"),
            field("NWR1", "predecessorSealKey", "434+N+A+C+P+E+R", "2+S", "u16+UTF-8", "if present,1..1024"),
            field("NWR1", "predecessorSealSha256", "436+N+A+C+P+E+R+S", "32", "bytes", "if present,non-zero"),
            field("NWR1", "terminalProtocolCheckpointPresent", "468+N+A+C+P+E+R+S", "1", "u8", "0 or 1"),
            field("NWR1", "terminalProtocolKind", "469+N+A+C+P+E+R+S", "1", "u8", "Kafka=1 if present"),
            field("NWR1", "terminalProtocolHeadKey", "470+N+A+C+P+E+R+S", "2+T", "u16+UTF-8", "if present"),
            field("NWR1", "terminalProtocolHeadValueSha256", "472+N+A+C+P+E+R+S+T", "32", "bytes", "if present"),
            field("NWP1", "rootReference", "8", "46+R", "reference", "exact Root key/SHA/shard/epoch"),
            field("NWS1", "rootReference", "8", "46+R", "reference", "exact Root key/SHA/shard/epoch"),
            field("NWS1", "terminalVector", "54+R", "24", "3*i64be", "each -1..Long.MAX"),
            field("NWS1", "finalHeadKey", "78+R", "2+H", "u16+UTF-8", "1..1024"),
            field("NWS1", "finalHeadSha256", "80+R+H", "32", "bytes", "non-zero"),
            field("NWS1", "aggregateExtentCount", "112+R+H", "8", "i64be", "non-negative"),
            field("NWS1", "aggregateCanonicalBodyBytes", "120+R+H", "8", "i64be", "non-negative"),
            field("NWC1", "rootSha256", "8", "32", "bytes", "non-zero"),
            field("NWC1", "pageOrdinal", "40", "8", "i64be", "non-negative"),
            field("NWC1", "predecessorPresent", "48", "1", "u8", "0 or 1"),
            field("NWC1", "predecessorPageSha256", "49", "32 if present", "bytes", "page0 absent;later present"),
            field("NWC1", "rowCount", "49+X", "2", "u16be", "1..256"),
            field("NWC1", "rows", "51+X", "sum(56+T[i])", "ordered structs", "proof token Root-capped"),
            field("NWC1", "coveredThrough", "51+X+ROWS", "24", "3*i64be", "exact row fold"),
            field("NWH1", "rootSha256", "8", "32", "bytes", "non-zero"),
            field("NWH1", "shardRunEpoch", "40", "8", "i64be", "non-negative"),
            field("NWH1", "publisherEpoch", "48", "8", "i64be", "non-negative"),
            field("NWH1", "pageOrdinal", "56", "8", "i64be", "-1 empty;non-negative with page"),
            field("NWH1", "pagePresent", "64", "1", "u8", "0 or 1"),
            field("NWH1", "pageReference", "65", "34+H if present", "u16+key+SHA", "exact page identity"),
            field("NWH1", "coveredThrough", "65+Y", "24", "3*i64be", "empty iff no page"));

    private static final List<ClosedCode> FORMAT_CODES = List.of(
            code(0, "nwg1ManifestVersion", "NWG1 manifest v1"),
            code(1, "headerLayoutVersion", "256-byte NWG1 Header v1"),
            code(2, "directoryLayoutVersion", "NWD1 directory v1"),
            code(3, "bindingContextRowVersion", "116-byte binding-context row v1"),
            code(4, "kafkaAppendUnitRowVersion", "104-byte Kafka append-unit row v1"),
            code(5, "pulsarAppendUnitRowVersion", "96-byte Pulsar append-unit row v1"),
            code(6, "commonFrameRowVersion", "48-byte common frame row v1"),
            code(7, "bindingEpochValidationKind", "full typed binding validation policy"),
            code(8, "bindingEpochValidationVersion", "NSE1 ordinal-zero plus owner/Kafka epoch validation v1"),
            code(9, "leafKeyGrammarVersion", "fixed one-digit lane and 19-digit fields v1"),
            code(10, "laneCatalogVersion", "0 latency,1 balanced,2 cost"),
            code(11, "planThenSequenceContractVersion", "seal plan before allocating sequence"),
            code(12, "packingPolicyCatalogVersion", "Root-compatible packing policy catalog v1"),
            code(13, "frameCodecRegistryKind", "closed per-frame codec registry"),
            code(14, "frameCodecRegistryVersion", "NONE/ZSTD registry v1"),
            code(15, "allowedFrameCodecsVersion", "NONE and ZSTD admitted set v1"),
            code(16, "objectDigestKind", "SHA-256"),
            code(17, "objectDigestVersion", "SHA-256 v1"),
            code(18, "payloadChecksumKind", "CRC32C"),
            code(19, "payloadChecksumVersion", "CRC32C v1"),
            code(20, "aeadKind", "AES-256-GCM-TAG128"),
            code(21, "aeadVersion", "AES-256-GCM-TAG128 v1"),
            code(22, "kdfKind", "HKDF-SHA256-OBJECT-INFO"),
            code(23, "kdfVersion", "HKDF-SHA256-OBJECT-INFO v1"),
            code(24, "nonceLayoutVersion", "NWG1 deterministic nonce layout v1"),
            code(25, "rootEnvelopeKind", "KMS-WRAPPED-WALRUN-KEY"),
            code(26, "rootEnvelopeVersion", "KMS-WRAPPED-WALRUN-KEY v1"));

    private static final List<Field> ENVELOPE_FIELDS = List.of(
            field("NWE1", "kind", "0", "2", "u16be", "exactly 1"),
            field("NWE1", "version", "2", "2", "u16be", "exactly 1"),
            field("NWE1", "canonicalLength", "4", "4", "u32be", "exact remaining bytes"),
            field("NWE1", "providerIdLength", "8", "4", "u32be", "1..64"),
            field("NWE1", "wrappingAlgorithmIdLength", "12", "4", "u32be", "1..64"),
            field("NWE1", "wrappingKeyIdLength", "16", "4", "u32be", "1..4096"),
            field("NWE1", "wrappingKeyVersionLength", "20", "4", "u32be", "1..1024"),
            field("NWE1", "wrappedKeyLength", "24", "4", "u32be", "1..16384"),
            field("NWE1", "providerId", "28", "K1", "ASCII token", "closed printable token"),
            field("NWE1", "wrappingAlgorithmId", "28+K1", "K2", "ASCII token", "closed printable token"),
            field("NWE1", "wrappingKeyId", "28+K1+K2", "K3", "ASCII", "immutable exact identity"),
            field("NWE1", "wrappingKeyVersion", "28+K1+K2+K3", "K4", "ASCII", "immutable,not current"),
            field("NWE1", "wrappedKey", "28+K1+K2+K3+K4", "K5", "bytes", "opaque exact wrapped key"));

    private WalRunControlWireProjectionV1() {}

    public static List<Field> fields() {
        return FIELDS;
    }

    public static List<Field> envelopeFields() {
        return ENVELOPE_FIELDS;
    }

    public static List<ClosedCode> formatContractCodes() {
        return FORMAT_CODES;
    }

    public static CanonicalBytes canonicalTsv() {
        StringBuilder result = new StringBuilder("record\tfield\toffset\twidth\tencoding\tconstraint\n");
        for (Field field : FIELDS) {
            result.append(field.record())
                    .append('\t')
                    .append(field.name())
                    .append('\t')
                    .append(field.offsetExpression())
                    .append('\t')
                    .append(field.widthExpression())
                    .append('\t')
                    .append(field.encoding())
                    .append('\t')
                    .append(field.constraint())
                    .append('\n');
        }
        for (Field field : ENVELOPE_FIELDS) {
            result.append(field.record())
                    .append('\t')
                    .append(field.name())
                    .append('\t')
                    .append(field.offsetExpression())
                    .append('\t')
                    .append(field.widthExpression())
                    .append('\t')
                    .append(field.encoding())
                    .append('\t')
                    .append(field.constraint())
                    .append('\n');
        }
        for (ClosedCode code : FORMAT_CODES) {
            result.append("NWR1_FORMAT\t")
                    .append(code.name())
                    .append('\t')
                    .append(code.index())
                    .append("\t1\tu8\texactly ")
                    .append(code.value())
                    .append(';')
                    .append(code.semantics())
                    .append('\n');
        }
        return CanonicalBytes.copyOf(result.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static Sha256Digest projectionSha256() {
        return Sha256Digest.hash(canonicalTsv());
    }

    private static Field field(
            String record, String name, String offset, String width, String encoding, String constraint) {
        return new Field(record, name, offset, width, encoding, constraint);
    }

    private static ClosedCode code(int index, String name, String semantics) {
        return new ClosedCode(index, name, 1, semantics);
    }

    public record Field(
            String record,
            String name,
            String offsetExpression,
            String widthExpression,
            String encoding,
            String constraint) {}

    public record ClosedCode(int index, String name, int value, String semantics) {}
}
