# M1.1c-R0 Registry capacity readiness evidence

## Result boundary

`REGISTRY_CAPACITY_READINESS_ONLY`; `promotionEligible=false`; `registryConformance=false`.

This deterministic test/evidence-only artifact binds Nereus `c8cb92ff8aeff7859acdc20097bb3d018d4eb792`, 18 focused tests, the exact
184 + writerCount * 120 + assignment-row-sum formula, and the full bounded cohort lifecycle. It does
not implement R1 production authority, select an allocator, run real Oxia, promote any V2-POSITION
scenario, or emit `REGISTRY_CONFORMANCE`/`HARNESS_CONFORMANCE_ONLY`.

## Derived boundary

- writer kinds: 2;
- source-qualified independently revocable cohort slots per kind: 7;
- `maxWriterCount=14`;
- maximum canonical Registry value and Oxia CAS candidate value: 51,016 bytes;
- expected-version operand: 8 bytes; combined value/version operands: 51,024 bytes;
- inherited envelope: 65,536 bytes; reserved margin: 14,520 bytes;
- exact boundary errors: `REGISTRY_WRITER_COUNT_EXCEEDED` and
  `REGISTRY_CANONICAL_BYTES_EXCEEDED`.

The 120-byte writer row, 192-byte full assignment-row contribution, and 256 lifetime-assignment limit
are unchanged. The margin cannot admit a fifteenth writer or hidden field.

## Artifact identity

- JSON: `registry-capacity.json`
- JSON SHA-256: `245207b8df4fefa639bc740a59706155544c7e9e587931960c7ec81c60a7c5d2`
- required baseline: `26728bec826ac72e5b893d4d19983d588feaca4f`
- expected focused tests: 18, failures/errors/skipped: 0/0/0
