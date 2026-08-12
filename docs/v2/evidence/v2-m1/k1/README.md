# K1 Kafka KRaft metadata-authority focused evidence

This directory binds Kafka K1 to the clean source-locked fork commit
`8afbc425660f3466bdc3255e3dd4eb43f8685af1`, descended from the accepted implementation base
`76f62f3b83e882105219b6c7687dbde594a8b8a2`, and to the immutable N1 domain input. The fork commit and its seven
reviewable K1 commits are pushed to `origin/nereus/future9-native-kafka-storage`.

`v2M1K1FocusedCheck` runs 39 exact tests in 16 JUnit suites with zero failure, error, or skip. It verifies feature 2,
API-key-32000 wire v0, direct domain mapping, CreateTopics resolution and exact count/byte admission, KRaft image and
snapshot authority, publication failure, Admin projection and mutation rejection, internal-topic/remote-log policy,
and the fresh Raft batch seam. It also checks the clean source before and after, exact N1 JAR/POM/manifest hashes,
generated schema inventory, and the absence of SPI/Oxia/dynamic dependency and Produce/Fetch integration.

This is `K1_FOCUSED_ONLY` evidence with `promotionEligible=false`. It does not activate a data path, promote a scenario,
prune V1, complete N2/N3, or claim M1 PASS.
