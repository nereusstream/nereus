# N1 immutable domain/SPI artifact evidence

This directory records the immutable N1 input selected for Kafka K1, Pulsar P1, and Registry R1. The source commit is
`330aaec349c51fb2ace52b1085e8a9e5a60b5e3e`; it was clean, already pushed to `origin/main`, and built twice under the
exact coordinate `0.2.0-n1.330aaec349c51fb2ace52b1085e8a9e5a60b5e3e`.

The two clean builds were byte-identical. The source-SHA bundle was published by absent-directory atomic rename and
contains two binary JARs, two source JARs, two POMs, two Gradle module metadata files, and two identity files. Their
combined size is 155,477 bytes; `manifest.sha256` is another 1,987 bytes with SHA-256
`9058ff01f9029f12d9fd2d0a7bc0456322bd5b2d19223a3961ee2201a07b91bb`.

`v2M1N1ArtifactCheck` validates the locked files, POM/module dependency boundary, reproducible ZIP metadata, exact
source ancestry, 119 discovered/executed/passed foundation tests with zero failure/error/skip, and this non-promotable
receipt. This evidence qualifies N1 only as an immutable
input. It does not implement K1/P1/R1, promote `sourceTupleId`, prune V1, or claim M1 PASS.
