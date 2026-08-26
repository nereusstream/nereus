# M3 K NWKCP1 preselection child

This directory records the exact-source, non-promotable K child produced from clean published Nereus source
`fc7aa79004f396b16d2a252231ec8b1179307318`.

The governed 5-test execution covers the production-codec fixture, strict NWKCP1 wire and Root-bound content key,
OPEN/TERMINAL `KafkaProtocolCheckpointHeadV1`, response-loss convergence and publisher takeover, exact physical
closure before terminalization, and fail-before-I/O bounded checkpoint recovery. The fixture contains a 324-byte
immutable NWKCP1 Object and 434-byte OPEN and TERMINAL Heads with exact key, SHA-256, and wire hex.

`receipt.json` has SHA-256 `864fdedc554796579e12e15a7b5aaa91be4dcef345b355fc5a93f89ec1f30c7f`.
Its governed JUnit attachment is `67368ae5c1031441d2e19121c32af21ae1be94c1c447642e84c6b01907c03a56`,
and its protocol fixture is `b19aebffd6f79fef99af9c0ed6933ac9975b2e60e058c144a578db845d0be841`.
The child does not substitute for the Kafka native Object-WAL child, activate native broker/controller paths,
promote a scenario, or provide M3 Final authority.
