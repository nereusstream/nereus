# M2 Kafka K0-M immutable module evidence

This receipt binds the first M2 production module graph to Nereus source commit
`cf465a637b960642b08287c5178ad45f2c0a240c` and coordinate
`0.2.0-m2.cf465a637b960642b08287c5178ad45f2c0a240c`. The clean, pushed source was built twice; all 16
published files were byte-identical and are covered by manifest SHA-256
`6827a8f6e32698ba24aaa81582a69778f3457d80b5486b871611bec3dc4802d4`.

`v2M2KafkaK0ModuleCheck` verifies the closed three-module graph, the exact immutable N1 coordinate, the exact Apache
BookKeeper `release-4.18.0` source/Maven input, all locked bundle bytes, publication metadata, and three executed tests
with zero failure, error, or skip.

The result is exactly `PASS_K0_M_INPUT_ONLY`. It does not qualify a provider session, buffer ownership, `NBKE2`,
numeric admission, Kafka integration, any scenario, `v2M2KafkaInputsCheck`, or global `v2M2Check`.
