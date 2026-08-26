# M3 C1 real Provider/KMS preselection child

This directory records the exact-source, non-promotable C1 child produced from clean published Nereus source
`e4d207e3e0526e85fece0401497a18f5e73d226c`.

- MinIO `RELEASE.2025-09-07T16-13-09Z` ran at the locked image/config digests and exercised the 64-MiB C1 root cap,
  conditional create, exact replay/conflict, range reads, paginated strong LIST/absence, and deterministic
  response-unknown cuts.
- HashiCorp Vault Transit `1.20.4` ran at the locked image/config digests in dev mode and exercised SPI wrap/unwrap,
  versions 1 and 2, old-version decrypt after rotation, terminal-closure-gated rotation, and session close.
- The governed ordinary summary additionally binds the required `C1ObjectProviderSessionTest` and
  `KmsCellSessionTest`. All four testcases report zero failure, error, and skip.

`receipt.json` has SHA-256 `184fa0e2e470d2800d8b504425cac0b5d9236f26e57c304bf37510e7e8cca180`.
It has `promotionEligible=false`, performs no C2 claim, does not prove a production Vault deployment, and does not
promote any scenario. ADR 0105 requires a fresh rerun after a uniquely qualified allocator mode changes the source
lock from `UNSELECTED`.
