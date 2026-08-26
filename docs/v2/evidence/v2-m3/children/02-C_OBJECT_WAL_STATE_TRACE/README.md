# M3 C Object-WAL state-trace preselection child

This directory records the exact-source, non-promotable C child produced from clean published Nereus source
`1d181dc65e86f1cc9da6d0a6bd840b8fffee21bf`.

The governed execution replays all 50 authored deterministic traces and verifies the closed 21-outcome inventory,
fault classes, call profiles, counters, budgets, and isolation fields. Both governed test classes pass 7/0/0/0.

`receipt.json` has SHA-256 `ed5b131eb22ecc03e0ec698723b8fad30ade32cb27e04dbe1f219224bc0c2f4a`.
Its governed JUnit attachment is `505cdf457c858034050121f01495cd4766f5e08f4b6e4de266be3cc20469dbb6`,
and its exact 50-row trace manifest is `b176f32ab2d28184c140bfbaf82d2f4b0daa9f3db5f0d5b654acccc6ae9f8b19`.
The child proves only the common deterministic state kernel and manifest. It does not substitute for Kafka/Pulsar
native backend evidence, promote a scenario, or provide M3 Final authority.
