# Pulsar M2-P6 provider and block-policy evidence

This directory binds the P6 selection to Nereus source
`4af3278234d84df7a2fdce4fc6b3e4e227916d56` and Pulsar source
`a14e0e6f4e49be0677318b4ceefc7b85b445823b`.

`candidate-matrix.json` is the production NPD1/NPO1 path on the AWS SDK v2 adapter against
`localstack/localstack:4.14.0`. It contains every 1/4/8/16-MiB target with `FIXED_NONE` and
`ZSTD_IF_SMALLER` for both the 50,000-by-100-byte ledger and a 20-MiB scan ledger. It also covers the stock 5-MiB
message and a 64-MiB-minus-1-KiB dedicated entry. Every case records random and sequential latency, provider
request/byte counts, provider p50/p99, process CPU, compression ratio, observed heap/direct memory, and concurrency 4.

`native-baseline.json` is the pinned Pulsar jcloud path against a dedicated LocalStack container. Its 1-MiB transfer
unit is source-declared by `TieredStorageConfiguration`; the receipt does not misstate it as wire-observed bytes.
`minio-provider.json` proves conditional create, bounded range reads, deletion/absence, multipart abort/relist, and the
canonical NPD1/NPO1 round trip against the fixed MinIO provider. `execution.json` binds the zero-failure/zero-skip
reports, container image identities, host, and provider operation counts.
`provider-capability-source.md` separates published Amazon S3 limits and conditional-write semantics from the local
S3-compatible execution.

## Selection

P6 selects exactly three common typed classes:

- `latency-1mib` -> 1 MiB;
- `balanced-4mib` -> 4 MiB and the Deployment base default;
- `scan-8mib` -> 8 MiB.

The 16-MiB candidate is rejected. On the 20-MiB workload it saved only one provider request relative to 8 MiB while
materially increasing exact-entry read amplification; it did not improve the measured sequential result. The 4-MiB
default reduces provider requests relative to 1 MiB while avoiding the larger 8/16-MiB random-read unit. These values
are operational block-close targets, not minimum block sizes.

The selected hard envelope is 4 GiB per data Object, 1,024 multipart parts, 64 MiB per entry and decoded block, and
65,536 entries per block. Provider admission must still prove the lower Cell/deployment limits against the concrete
provider. The Topic cannot raise them.

## Claim boundary

P6 proves the production adapter behavior, bounded codecs/read path, policy resolution, exact candidate selection, and
native-relative cold-read cost for this source tuple. MinIO admission is exact-release scoped, and LocalStack is not
Amazon S3; this directory does not claim Amazon S3 latency, durability, availability, or endorsement. P6 also does not
prove broker/NAR process wiring,
Pulsar Final scenario promotion, M8 performance acceptance, or global M2 PASS.
