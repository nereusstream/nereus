# Pulsar M2 Final evidence

`pulsar-final.json` is the canonical production receipt for the Pulsar-owned M2 sub-aggregate. It binds tested Nereus
source `4af3278234d84df7a2fdce4fc6b3e4e227916d56`, exact Pulsar fork
`a14e0e6f4e49be0677318b4ceefc7b85b445823b`, the current Kafka Final and P6 roots, and 32 exact suite references for
eleven BookKeeper scenarios.

The three summary attachments account for 9/85 local functional tests, 2/26 exact-fork native tests, and 3/5 P6
provider tests. The other five attachments are the current Kafka Final receipt, P6 candidate matrix, execution receipt,
native baseline, and fixed MinIO provider result. All named results have zero failures, errors, and skips.

This receipt does not promote mixed `V2-BK-011`, activate a broker/NAR process, claim Amazon S3 performance or
endorsement, establish M8 native feature/performance parity, or by itself claim the separate global M2 aggregate.
