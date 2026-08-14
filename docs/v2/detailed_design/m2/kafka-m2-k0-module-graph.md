---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeImplementationSlice
sourceTuple: v2-m1
---

# M2-K0-M module graph and immutable N1 input

K0-M is the first production M2 slice. It adds exactly `nereus-storage-api`, `nereus-storage-bookkeeper`, and
`nereus-kafka-bookkeeper`. The storage API compiles against the immutable N1 domain coordinate
`0.2.0-n1.330aaec349c51fb2ace52b1085e8a9e5a60b5e3e`; it does not compile against the mutable project
`:nereus-domain`. The BookKeeper adapter pins `bookkeeper-server:4.18.0`. The Kafka engine module sees the storage API
and adapter but adds no Kafka runtime activation.

`v2M2KafkaK0ModuleCheck` executes one non-zero boundary suite in every module, validates exact N1 and BookKeeper JAR
linkage, generates all three binary/source JARs plus POM/Gradle metadata, and checks the filtered source-qualified M2
publication guard. The source-qualified bundle is produced only from a clean pushed implementation commit and refuses
an existing destination. Its receipt and source-lock binding land as evidence descendants of that tested source.

This slice does not implement a provider session, buffer ownership, `NBKE2`, checked admission, a run writer, native
Kafka transport, a scenario PASS, `v2M2KafkaInputsCheck`, or global M2 Final. Those absent gates remain absent.
