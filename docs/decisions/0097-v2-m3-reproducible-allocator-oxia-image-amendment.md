# ADR 0097: V2 M3 reproducible allocator Oxia image amendment

- Status: Accepted
- Date: 2026-08-24
- Amends: ADR 0094's allocator executor/source attachment only
- Preserves: ADRs 0088, 0091, 0093, 0094, the M1 O1/R1/P1 receipts, and all production wire/state contracts

## Context

The accepted M1 Oxia evidence records local image `nereus/oxia-o1:37a17bef1720` with config digest
`sha256:5aa715e4f19091931743e5af489af5f8d6ee15efcce6430a908c6f65cc6d6516`. That image was explicitly local-only and
was not published. Its historical receipt and source-lock entries remain immutable.

M3 allocator preflight initially attempted to reuse that local tag and digest. A current audit found that the exact
historical config object is no longer present locally. Rebuilding the old Dockerfile cannot recreate it: its
`golang:1.26-alpine` and `alpine:3.22` references were floating tags, package installation/upgrade was unpinned, image
timestamps were not normalized, and a linked worktree cannot provide the original in-container `.git` object graph.
The accepted O1 server source commit and binary version remain available and unchanged. Relabelling a new build with
the historical digest, changing the M1 lock, or accepting any same-tag image would falsify provenance.

## Decision

M3 allocator evidence uses an independently versioned evidence image. It does not replace, supersede, or rerun the M1
image receipt:

| Field | Exact M3 value |
| --- | --- |
| source repository | `https://github.com/nereusstream/oxia.git` |
| dedicated evidence checkout branch | `nereus/v2-m3-object-wal-evidence` |
| exact published source ref | `refs/heads/main` |
| Oxia source commit | `37a17bef17202d5fd6e23282da5fd26d94865484` |
| binary version | `oxia version 0.16.3-167-g37a17bef` |
| image reference | `nereus/oxia-m3-allocator:37a17bef1720` |
| image config digest | `sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da` |
| image recipe | `scripts/containers/oxia-v2-m3-allocator.Dockerfile` |
| recipe SHA-256 | `31388e201ce95fd61c1505a8628a66993ec8c070ba02ad5f71aa647ae066d238` |
| source epoch | `1786412361` (`2026-08-11T01:39:21Z`) |
| platform | `linux/arm64` |

The recipe pins both base-image manifest digests, disables Go VCS auto-stamping, fixes the exact accepted version,
removes the Go build ID, trims build paths, normalizes the three copied file timestamps, and installs no runtime
packages. `scripts/build-v2-m3-allocator-oxia-image.sh` accepts only the clean dedicated Oxia worktree branch while
also requiring the independently published `origin/main` source ref at the exact source commit; the local evidence
branch itself is not treated as published provenance. It rebuilds the image and requires the exact config digest,
labels, platform, and binary version. A no-cache build followed by a normal independent export produced the same
full-label config digest above; a truncated source label produces a different digest and is rejected. No Oxia or
Oxia-client product-source change is part of this amendment.

The M3 allocator source tuple also advances the Pulsar input from the historical M2 native commit to the clean pushed
M3 branch `nereus/v2-m3-object-wal-evidence@7ff908330809f2e9bc5c69ead87bb85c566bc0a9`. The M2 source-lock entry remains
unchanged. `m3AllocatorEvidenceBinding` in `docs/v2/source-locks.json` is the only image/source-lock authority used by
M3 allocator preflight.

## Consequences

- Formal allocator evidence must build and verify the M3 image before starting Oxia or Gradle. A missing image, wrong
  config digest, label, recipe SHA, source branch/ref, platform, or binary version blocks admission.
- The preflight receipt records `historicalM1ImageReplacement=false` and
  `independentlyVersionedM3EvidenceImage=true`.
- This amendment changes no Oxia product source, allocator mode/range selection rule, `NVAC1`/`NVAH1`/`NVAN1`,
  `NAEA1`/`NARS1`, Object-WAL wire, scenario status, or M1/M2 receipt.
- The image remains a local formal evidence attachment. Production Oxia packaging/deployment is not claimed by M3.
