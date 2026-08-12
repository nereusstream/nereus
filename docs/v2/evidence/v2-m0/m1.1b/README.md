# M1.1b production NTA1 v1 exact-local evidence

This receipt binds the accepted NTA1 v1 production codec, strict validator, exact goldens, pure-input Pulsar
name-inventory admission, and O2 aggregate-codec adapter to Nereus implementation commit
`01a70f17ec9176385e04242490a5fa4f6b230dda`.

The result is `PASS_LOCAL_NTA1_CODEC_ONLY`, with `promotionEligible=false`. At receipt close the domain module had 13
suites/55 tests and the O2 namespace had 10 suites/73 tests; the complete `nereus-metadata-oxia` module had 73
suites/303 tests. All had zero failure, error, and skip. The structured owner is
[implementation.json](implementation.json).

The six production goldens cover Kafka/Pulsar minimum, typical, and boundary vectors. Small vectors retain exact hex in
the test resource; boundary vectors retain length, SHA-256, and fixed prefix/suffix. The largest legal classic-persistent
Pulsar vector is 8,395 bytes under the exact 8,397-byte parser cap.

This is not K1, P1, R1, a real Oxia/Pulsar conformance receipt, an existing-cluster inventory scan, runtime activation,
scenario promotion, M1 PASS, or M1 Final. The historical Q1 receipt remains unchanged as
`READINESS_EVIDENCE_ONLY`.
