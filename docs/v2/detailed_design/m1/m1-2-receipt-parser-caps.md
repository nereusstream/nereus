---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: ImplementationDesign
sourceTuple: v2-m0
receipt: docs/v2/evidence/v2-m0/m1-2-receipt-caps/README.md
---

# M1-2 receipt/parser capacity and attachment-safety boundary

## Role and sequencing boundary

This is the code-level design for the receipt/parser-cap slice between M1.1c-R0 and immutable N1 artifact
publication. It has closed the numeric inputs needed by the later G1 receipt validator. It does not implement G1, register
`v2M1Check`, `v2M1ExactSourceCheck`, or `v2M1FinalCheck`, publish N1, enter K1/P1/R1, prune V1, or promote a scenario.

[ADR 0084](../../../decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md) remains the normative receipt
authority. ADRs 0081, 0083, and 0085 own the surrounding promotion, JCS, attachment, and Final-reference invariants.
This design owns only the parser call order, bounded evidence model, deterministic samples, exact cap-selection rule,
test layout, and fast-gate delivery order. An implementation conflict with an accepted ADR stops this slice.

The starting baseline is clean `origin/main@7ede023e19774309268350a866932804787a52a7`. The slice is deliberately
test/evidence-only. Production `nereus-domain` and `nereus-metadata-spi` receive no receipt parser, generic JSON API,
filesystem facade, runtime authority, or promotion mutation. G1 later consumes the accepted constants and schema in a
separately reviewed JDK-only evidence-domain implementation after K1/P1/R1 exist.

## Preserved receipt and Final authorities

The receipt root remains exactly these five members:

```text
schema
kind
sourceTuple
scenarios[]
attachments[]
```

The closed receipt kinds remain `REGISTRY_CONFORMANCE` and `HARNESS_CONFORMANCE_ONLY`. The latter derives
`selectionEligible=false` from its kind; the boolean is not supplied in JSON. There is no `runIdentity`, timestamp,
leaf ID, retry result, independent scenario summary, or aggregate result.

`scenarios[] -> suites[]` remains the sole test-result authority. A later Final index remains only
`schema + sourceTupleSha + requiredGateRefs[] + receiptRefs[]`, with typed path/length/SHA-256 references. M1-2 does
not create, parse, or masquerade as that index and does not emit an N2/N3 receipt.

## Exact v1 root schema consumed by the future parser

The root `schema` value is exactly `NEREUS_VIRTUAL_LEDGER_RECEIPT_V1`. All root strings are schema-bounded ASCII;
human-readable output, stack traces, and logs stay in attachments. Canonical bytes use RFC 8785/JCS: UTF-8 without a
BOM, no insignificant whitespace, object members sorted by the JCS rule, canonical integer spelling, and canonical
string escaping. The parser rejects duplicate or unknown members, floating point, negative numbers, exponent form,
leading zeroes, invalid escapes, invalid UTF-8, and any input whose closed-model JCS re-encoding differs byte-for-byte.

`sourceTuple` contains exactly:

```text
nereusCommit
kafkaCommit
pulsarCommit
oxiaClientCommit
oxiaServerCommit
oxiaClientJarSha256
oxiaClientPomSha256
domainJarSha256
domainPomSha256
oxiaServerImageDigest
sourceLocksSha256
```

The five commits are 40 lowercase hexadecimal bytes. The five plain digests are 64 lowercase hexadecimal bytes; the
server image digest is `sha256:` plus 64 lowercase hexadecimal bytes. A source-tuple member is never an attachment,
URL, branch, tag, host path, or mutable source alias.

Each scenario is exactly `{scenarioId,suites}`. `scenarioId` matches the existing structured-scenario grammar and is
at most 64 ASCII bytes. Scenarios are strictly increasing by ID and unique. Each suite is exactly:

```text
suiteId
discovered
executed
passed
failed
skipped
aborted
```

`suiteId` is 1..256 bytes from `[A-Za-z0-9._:/-]`, begins with an alphanumeric byte, and is strictly increasing and
unique within its scenario. Counts are canonical JSON integers in `0..2^53-1`. Every addition/subtraction uses checked
`long` arithmetic and enforces:

```text
discovered = executed + skipped
executed   = passed + failed + aborted
```

Containers are not leaves. Parameterized invocations are leaves. Existing JUnit `failures` and `errors` normalize by
checked addition into receipt `failed`; they are not persisted as competing fields. Dynamic tests and internal retries
remain forbidden. A receipt may faithfully record a zero, skipped, failed, or aborted result, but the derived mandatory
PASS check rejects zero discovered/executed, any failed/skipped/aborted count, or a missing required suite. Parsing a
failure receipt is not promotion.

Each attachment is exactly `{attachmentKind,length,path,sha256}`. Kinds remain closed to `TEST_REPORT`,
`REGISTRY_BYTES`, `REGISTRY_ADMISSION_EVIDENCE`, `WRITER_INTERLOCK_SNAPSHOT`, and `SANITIZED_LOG_EXCERPT`. `length` is
the declared byte length, not a character count or allocation request. Paths are strictly increasing and unique.

## Accepted v1 hard caps and derivation evidence

[ADR 0084](../../../decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md) now owns the sole normative
numeric table. The exact machine projection is
[`receipt-caps.json`](../../evidence/v2-m0/m1-2-receipt-caps/receipt-caps.json); this design intentionally does not
maintain a second cap table.

The required-baseline JUnit inventory measures 91 suites, 386 tests, and 96,248 XML bytes. The generated samples
observe maxima of 16,079 canonical-root bytes, seven scenarios in one kind-specific root, 73 suites per scenario, 20
attachments, 84,025 bytes in one generated attachment, 158,760 total bytes in the kind-complete bundle, a 115-byte
five-segment path, and a 15,425-byte sanitized log. The Registry fixture is the exact structured 51,016-byte R0
maximum. The evidence JSON records and the gate recomputes every rounding/composition formula; a drift fails rather
than silently changing an ADR cap. No representative artifact uses anonymous repeated filler.

The fourfold root margin conservatively composes the independently closed scenario axis with the largest observed
suite/attachment root. Report/Registry, total-bundle, path, segment, and sanitized-log margins use the exact formulas
in the evidence. These are persisted-v1 parser invariants: Deployment may lower only new-receipt admission, while a
host, CI runner, upload service, environment, or provider cannot enlarge, lower, or reinterpret parsing of persisted
v1 bytes. Expansion requires a new schema.

## Parser input and validation order

The future G1 parser accepts one already identified receipt-root regular file or a bounded byte source plus the exact
receipt directory. M1-2 models that call without installing production code. Validation order and stable precedence
are:

1. reject a missing/non-regular/symlink root and a declared or streamed root above 65,536 bytes before `readAllBytes`;
2. reject BOM, invalid UTF-8, malformed JSON, duplicate members, depth/type errors, and non-integer number syntax;
3. parse only the closed five-field model, reject unknown/missing fields, then JCS-reencode and compare exact bytes;
4. validate schema/kind/source tuple and all fixed string grammars;
5. cap scenario, per-scenario suite, and attachment arrays before allocating their backing collections;
6. validate sorted unique scenario/suite/path identities and checked count equations;
7. validate path bytes/segments, declared per-kind/file limits, and checked total attachment bytes before filesystem I/O;
8. resolve and stream-verify each attachment without following links; compare exact length and SHA-256;
9. derive scenario/overall outcome and apply required-suite/mandatory-PASS policy outside syntactic parsing.

An earlier structural error wins over a later semantic or filesystem error. A declared length over a cap wins before
file existence; path grammar wins before path resolution; attachment total overflow wins before opening the first
file. Mandatory-PASS failure never rewrites or hides the parsed counts.

## Path normalization, traversal, and symlink defense

Receipt paths are ASCII POSIX-relative paths under the receipt directory. The grammar rejects an absolute path,
leading/trailing slash, empty segment, `.`, `..`, backslash, colon, NUL, DEL/control byte, percent-encoded separator,
and any byte outside `[A-Za-z0-9._/-]`. Each non-dot segment begins with an alphanumeric byte. The canonical string is
used directly; there is no Unicode normalization, platform-separator conversion, URL decode, case fold, or trim.

The verifier first obtains a non-symlink real receipt directory. It walks every ancestor with
`LinkOption.NOFOLLOW_LINKS`, requires directories until the final component, opens the final component with READ and
NOFOLLOW semantics, and requires one regular file rather than a symlink, directory, FIFO, socket, or device. It then
streams at most the declared/capped length through SHA-256, rejects early EOF or one extra byte, and rechecks the open
file identity/attributes before accepting it. The production G1 implementation must use a `SecureDirectoryStream`
when the provider offers one or an equivalent open-handle/file-key recheck; a lexical `normalize().startsWith(root)`
test alone is insufficient against symlink substitution.

## Stable error categories

M1-2 freezes categories rather than exception prose. The future parser may attach non-authoritative detail, but gates
match these codes and the precedence above:

```text
RECEIPT_ROOT_NOT_REGULAR
RECEIPT_ROOT_BYTES_EXCEEDED
RECEIPT_MALFORMED_JSON
RECEIPT_NON_CANONICAL_JSON
RECEIPT_DUPLICATE_FIELD
RECEIPT_UNKNOWN_OR_MISSING_FIELD
RECEIPT_WRONG_TYPE_OR_NUMBER
RECEIPT_SCHEMA_OR_KIND_INVALID
RECEIPT_SOURCE_TUPLE_INVALID
RECEIPT_SCENARIO_COUNT_EXCEEDED
RECEIPT_SUITE_COUNT_EXCEEDED
RECEIPT_ATTACHMENT_COUNT_EXCEEDED
RECEIPT_DUPLICATE_OR_UNSORTED_ID
RECEIPT_ACCOUNTING_INVALID
RECEIPT_PATH_INVALID
RECEIPT_PATH_BYTES_EXCEEDED
RECEIPT_PATH_SEGMENTS_EXCEEDED
RECEIPT_ATTACHMENT_BYTES_EXCEEDED
RECEIPT_ATTACHMENT_TOTAL_BYTES_EXCEEDED
RECEIPT_SANITIZED_LOG_BYTES_EXCEEDED
RECEIPT_ATTACHMENT_NOT_REGULAR
RECEIPT_ATTACHMENT_SYMLINK
RECEIPT_ATTACHMENT_LENGTH_MISMATCH
RECEIPT_ATTACHMENT_DIGEST_MISMATCH
RECEIPT_CHECKED_ARITHMETIC_OVERFLOW
RECEIPT_REQUIRED_SUITE_MISSING
RECEIPT_MANDATORY_RESULT_NOT_PASS
```

## Deterministic capacity sample inventory

The harness renders strict canonical receipt roots plus content-addressed attachments for these named samples:

| Sample | Real input or generation rule | What it measures |
| --- | --- | --- |
| Foundation | current domain and metadata-SPI JUnit suite/count inventory | two-module all-pass normalization |
| O1 | locked 88/365/1 counts and real 10,058-byte exact-runtime XML fact | external fork/runtime identity and report size |
| O2 | current 73-suite/303-test whole module plus 10-suite/73-test V2 focus | largest current suite inventory |
| NTA1 | current 13-suite/55 domain, 10-suite/73 O2 focus, and six golden digests | exact-local multi-suite result |
| Registry readiness | current 18-test R0 result and exact 51,016-byte structured boundary fixture | maximum Registry attachment |
| representative all-pass | the measured required-baseline focused gate inventories, normalized once | ordinary PASS root/report |
| multi-scenario | all nine closed M1 virtual-ledger scenario IDs split by their required receipt kind | scenario bound without kind substitution |
| multi-suite | the 73 current O2 suite IDs and counts, not anonymous duplicate rows | suite/root bound |
| maximum failure | one distinct case for every frozen error/required-result category | failed/aborted accounting and diagnostic bytes |
| fault-cut | named O1 continuity, response-unknown, stale-install, Registry rollout, and allocator cut events | non-retried failure log shape |
| maximum Registry attachment | R0 184-byte header, 14 distinct 120-byte writer rows, and 256 distinct 192-byte assignment rows | exact 51,016 bytes and streaming digest |
| sanitized log | one sanitized structured line per named cut/error, with fixed non-secret identifiers | log-specific cap |

The harness never appends the same string until a target size is reached. Fixed-width binary rows contain their actual
index/identity fields plus domain-separated digests. Failure and log fixtures contain one independently named event per
covered cut or error. Maximum strings are used only where the closed field itself permits that boundary.

The evidence report records each sample's generator version, input source, root bytes/SHA, scenario/suite/attachment
counts, per-kind attachment bytes, total bytes, and observed maxima. It also records every selected cap, formula,
margin, focused test count, source commit, required baseline, source-lock input SHA, and JSON SHA. The Markdown is a
readable projection of the JSON and binds its SHA. Both are `RECEIPT_CAPACITY_READINESS_ONLY`,
`promotionEligible=false`, `productionReceiptParserImplemented=false`, `m1Final=false`, and
`scenarioPromotion=false`.

## Test and fast-gate plan

The JDK-only model lives under `nereus-domain/src/test/java/com/nereusstream/domain/receipt`. Focused tests cover:

- all canonical sample roots, deterministic repeat rendering, JCS member order, and exact reparse;
- root 65,536/65,537, scenario 16/17, suite 128/129, and attachment 32/33 boundaries;
- single 262,144/262,145, total 524,288/524,289, path 256/257, segment 16/17, and log 65,536/65,537 boundaries;
- checked addition/multiplication and `2^53-1` count overflow;
- unknown/missing/duplicate members, wrong types, floats/exponents/negative/leading-zero numbers, BOM, malformed UTF-8,
  whitespace/member-order/noncanonical escaping, and trailing bytes;
- zero discovery/execution, skip, failure, abort, missing suite, accounting mismatch, parameterized-leaf normalization,
  and `failures + errors -> failed` overflow;
- duplicate/unsorted scenario IDs, suite IDs, paths, unknown kinds, wrong source/digest types, and kind substitution;
- absolute/traversal/backslash/control/empty-segment paths, lexical escape, symlink target/ancestor, non-regular file,
  length mismatch, digest mismatch, and streamed extra byte;
- maximum structured Registry fixture, unique-row generation, maximum-failure and named fault-cut output, and no filler;
- absence from production source, no Docker/real Oxia, unavailable R1 codec, unchanged scenarios, and no Final task.

`v2M1ReceiptCapsCheck` runs only the focused tests, generated/committed byte equality, JSON parse, shell syntax, source
commit/baseline ancestry, source-lock input SHA, evidence SHA/length binding, production-absence checks, and
non-promotion guards. It fails on zero tests, any failure/error/skip, digest/source mismatch, nondeterministic output,
or a cap/formula drift. It is independent from `v2M1RegistryCapacityCheck` and does not become `v2M1Check`.

## Implementation record and stop boundary

The test-only JDK model, narrow parser, attachment verifier, representative generators, 36 focused tests, and
independent gate are source-bound to Nereus `75593faf11c5934908d6ffcd9977648f8fa49ea2`. The committed JSON/Markdown
are byte-identical to fresh generation; the JSON SHA-256 is
`2197c814dc887d742cdda119f4e68c4f5f2276df0f44b15de3d524a2445c692d`. The gate checks non-zero clean tests,
formula/cap stability, source ancestry, source-lock input identity, generated/committed equality, JDK-only scope,
production absence, and scenario non-promotion.

ADR 0084, the M1 index, plan/matrix, structured scenario readiness pointer, open questions, README, source locks, and
documentation checker are synchronized without changing any scenario to `PASSED_CURRENT_SOURCE`. This slice stops
here. N1 publication, K1/P1/R1, G1 production parser/Final, graph pruning, N2, and N3 need separate authorization.
