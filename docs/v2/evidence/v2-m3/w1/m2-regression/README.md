# M3 W1 current-source M2 regression

`receipt.json` is the non-promotable trusted/full M2 regression checkpoint produced at exact Nereus source
`89a766124c9ecd1ae407eb76024acddffbe19f69`. It binds all 25 closed child gates and 687 tests with zero failure,
error, or skip, including exact Kafka/Pulsar native sources, real BookKeeper, Kafka 10k/100k, and Pulsar P6
candidate/native/MinIO execution.

The receipt SHA-256 is `4a7e25060ff2e2700f1d4373b5ecb8123b5a51f587f0f80b0209ce4810dfd7bd`.
It keeps `promotionEligible=false`, `scenarioPromotion=false`, and `m2AmendmentLineage=[]`; it neither changes the
historical M2 Final nor promotes M3. M3 Final may consume only a complete W1 profile whose tested commit exactly
matches the eventual M3 tested source, so later non-evidence source changes require a full rerun and replacement
lineage rather than treating this checkpoint as fresh.
