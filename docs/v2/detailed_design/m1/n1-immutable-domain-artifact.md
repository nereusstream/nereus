---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: ImplementationDesign
sourceTuple: v2-m0
receipt: docs/v2/evidence/v2-m1/n1/README.md
---

# N1 immutable domain and metadata-SPI artifact

## Scope

N1 turns the exact-local M1 foundation into the immutable input consumed by Kafka K1, Pulsar P1, and Registry R1.
It publishes only `nereus-domain` and `nereus-metadata-spi`; it does not activate a backend, implement K1/P1/R1,
promote a scenario, prune V1, change `sourceTupleId`, or claim M1 PASS.

The source identity is a clean Nereus commit already present on `origin/main`. Publication uses the source-qualified
version:

```text
0.2.0-n1.<40-lowercase-hex Nereus source commit>
```

The immutable repository root is:

```text
gradle/locked-artifacts/nereus-n1/<40-lowercase-hex source commit>/m2
```

K1/P1/R1 must consume this exact repository root and coordinate. Dynamic `SNAPSHOT`, `changing=true`, Maven Local,
or a Nereus composite build cannot substitute for N1 evidence.

## Required artifact set

Each of `com.nereusstream:nereus-domain` and `com.nereusstream:nereus-metadata-spi` publishes exactly:

- binary JAR;
- source JAR;
- Maven POM;
- Gradle module metadata.

The domain POM has no production dependency. The metadata-SPI POM has exactly one production dependency: the exact
N1 `nereus-domain` version from the same bundle. No mutable Maven metadata file is part of the bundle.

The bundle also contains `source-commit.txt`, `coordinate-version.txt`, and a sorted `manifest.sha256`. The later N1
evidence commit records every relative path, byte length, SHA-256, and the manifest SHA in `source-locks.json` and an
append-only N1 receipt. `sourceTupleId` remains `v2-m0`; only N2 may promote the complete cross-repository tuple.

## Publication cut

N1 deliberately uses two commits so the artifact cannot self-lock the commit that records its own digest:

1. commit and push the N1 design, source-qualified version support, and publisher to `main`;
2. from that exact clean pushed commit, run two clean builds with identical source-qualified coordinates;
3. require byte-for-byte equality of all required artifacts and metadata;
4. stage the second result outside the final path and atomically rename it into the absent source-SHA directory;
5. refuse any attempt to publish when that final directory already exists;
6. validate and commit only the locked bundle, source-lock binding, receipt, gate, and truthful documentation state.

The second commit is evidence/lock metadata and is not the artifact source identity. Rebuilding from it under a new
SHA would create a different N1 candidate rather than overwrite the selected bundle.

## Gate and failure rules

`v2M1N1ArtifactCheck` must verify from a clean checkout:

- the bound source commit, coordinate, bundle root, and manifest agree;
- all eight Maven artifacts plus the two identity files exist, have non-zero bounded lengths, and match SHA-256;
- JARs are reproducible archives without timestamps or unexpected duplicate entries;
- the domain POM has no production dependency and the SPI POM pins only the exact bundled domain;
- neither POM nor Gradle metadata contains `SNAPSHOT`, a relative project dependency, or another repository;
- the source commit is an ancestor of the evidence commit and exists on `origin/main`;
- focused domain/SPI tests, dependency/API checks, and documentation checks execute with non-zero tests and no
  failure, skip, or abort.

Missing files, an existing-but-different target, dirty source, digest drift, a non-reproducible rebuild, a dynamic
coordinate, or a source-lock mismatch fails closed. N1 success remains an immutable input milestone, not M1 Final.

## Commit boundary

The source/publisher commit and the artifact/evidence commit stay separate. K1 starts only after the latter passes and
is pushed. Subsequent Kafka/Pulsar repositories record the N1 coordinate and artifact digests they actually resolved;
they must not rebuild N1 from their own checkout or silently select a newer bundle.

## Implementation record

N1 source commit `330aaec349c51fb2ace52b1085e8a9e5a60b5e3e` was pushed before publication. Two clean builds were byte-identical,
and the selected manifest SHA-256 is `9058ff01f9029f12d9fd2d0a7bc0456322bd5b2d19223a3961ee2201a07b91bb`.
The exact bundle and non-promotable receipt are locked by `docs/v2/source-locks.json`. This verifies N1 only as the
K1/P1/R1 artifact input; M1 remains `InProgress`.
