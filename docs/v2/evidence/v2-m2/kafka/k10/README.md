# Kafka M2-K10 Final evidence

`kafka-final.json` is the canonical production receipt for the Kafka-owned M2 sub-aggregate. It binds tested Nereus
source `4af3278234d84df7a2fdce4fc6b3e4e227916d56`, the exact Kafka and BookKeeper inputs, current-source K9 evidence,
and the exact named suites for ten Kafka-owned scenarios.

The two local attachments summarize the exact Kafka 4.3 conformance execution and the 22 current-source JUnit suites
actually referenced by the production promotion policy. The receipt also directly binds the K9 real-BookKeeper and
10k/100k scale attachments. `v2M2KafkaFinalCheck` revalidates the production resolver, the live suite reports, K2 and
K9 gates, source ancestry, evidence-only change boundary, and both scenario registries.

This receipt does not activate a Kafka broker runtime, prove native ISR/HW/election behavior, promote any mixed
M2/downstream scenario, complete Pulsar M2, or claim global M2 PASS.
