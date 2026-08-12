# M1.1a-O1 focused compatibility receipt

This receipt binds the O1 client fork to the exact server runtime. It is focused implementation evidence only and is
not a `REGISTRY_CONFORMANCE`, `HARNESS_CONFORMANCE_ONLY`, N2, N3, or M1 Final receipt.

- client final fork: `091a42c2780d92da56e9ec1f02ce1c3d988adc16`
- server source: `37a17bef17202d5fd6e23282da5fd26d94865484`
- server image: `nereus/oxia-o1:37a17bef1720`
- server image digest: `sha256:5aa715e4f19091931743e5af489af5f8d6ee15efcce6430a908c6f65cc6d6516`
- focused result artifact: `focused-compatibility.json`
- focused result artifact SHA-256: `80d59b7e9596d1ba17e05100aa16a26a69a73e709001e044e436a3da0bb887c4`
- raw JUnit XML: `TEST-io.oxia.client.it.NotificationContinuityCompatibilityIT.xml`, 10058 bytes, SHA-256
  `c34caebda3f85df5eb4627718ccd664ca51086144e62ea55383f3000bd9ba827`

Final clean-fork results:

- focused lifecycle/unit/API selection: 88 discovered, 88 executed, 88 passed, 0 failed, 0 errors, 0 skipped;
- full client repository check: 365 discovered, 365 executed, 365 passed, 0 failed, 0 errors, 0 skipped;
- exact runtime compatibility: 1 discovered, 1 executed, 1 passed, 0 failed, 0 errors, 0 skipped;
- exact runtime log identifies `nereus/oxia-o1:37a17bef1720`, not the default `oxia/oxia:0.16.3` image;
- the exact no-offset dummy established `ARMING -> READY`, and a later non-empty payload was decoded and delivered
  exactly once.

This result does not implement metadata-oxia O2, does not establish A/read/B or a Pulsar ownership fence, and cannot
promote `V2-META-006`, M1.1a, or M1.
