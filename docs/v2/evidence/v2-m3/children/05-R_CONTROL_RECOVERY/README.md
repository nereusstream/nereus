# M3 R control and recovery preselection child

This directory records the exact-source, non-promotable R child produced from clean published Nereus source
`35e6784d941e6fb2524644662dcfa86bc8ec9ddd`.

The governed 9-test execution covers strict Root/Pointer/Seal/checkpoint wire round trips, three lazy lanes, checkpoint
takeover and streaming recovery, exact Seal/successor lineage, bounded Root-owned tail inventory, and Provider/KMS
session closure with run-key erasure. One of the nine tests independently verifies the closed eight-row recovery
manifest, and the child validator rederives that manifest from the governed JUnit inventory.

`receipt.json` has SHA-256 `1863f29328f9ee5b37c7e36b53232eb841936427ba20c05175281692c2808f90`.
Its governed JUnit attachment is `31f7ff6e1b2ff65df3e417190300012dadf7fcedcb7e6c616375e8d7125ba871`,
and its recovery manifest is `131fb1b7564da8a310c1bc4609fc3ba4c258c90a461aed2694b5c2531973eebb`.
The child does not freeze a complete synthetic Root/Pointer wire, substitute for Kafka/Pulsar native evidence,
promote a scenario, or provide M3 Final authority.
