# Kafka M2-K9 real BookKeeper evidence

This directory contains the current-source K9 evidence for tested Nereus commit
`a7f82d7b2ac6adc6886336bb233bf3e7dddfc90d`. Both scale tiers used a fresh three-bookie cluster from the exact K0 image
and configuration, created and wrote one real ledger per logical partition, retained a bounded hot set, and exercised
targeted reads, fence/recovery, and rollover. The 100k tier is an actual 100,000-partition execution, not an
extrapolation from 10k.

The scale result files are direct harness outputs. `environment-summary.json` reduces the sampled Docker resource and
volume observations; `log-audit.json` records the raw-log digests and classifies only the exact expected fresh-volume
startup messages. `fault-tests.json` accounts for the six provider and three Kafka-engine real-BookKeeper cases.
`artifact-report.json` binds the reproducible production jars and current local/real test counts.

The sibling `k9-evidence.json` receipt binds every committed attachment by path, byte length, and SHA-256. K9 evidence
selects operational defaults but remains `promotionEligible=false`; K10, not this directory, owns the exact Kafka M2
scenario promotions and Kafka Final aggregate. Native Kafka runtime integration, M6 behavior, Pulsar M2 work, and
global M2 PASS are not claimed here.
