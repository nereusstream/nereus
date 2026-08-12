# M1.1a-O1 Oxia client artifact receipt

This is focused dependency evidence for source tuple `v2-m0`; it is not an M1 promotion receipt.

- repository: `https://github.com/nereusstream/oxia-client-java.git`
- implementation base: `24b730d1d66a1da701f4c99957361f6b3c5d748c`
- branch: `nereus/v2-m1.1a-o1-notification-continuity`
- final fork commit: `091a42c2780d92da56e9ec1f02ce1c3d988adc16`
- build: `./gradlew :client-api:jar :client-api:sourcesJar :client-api:generatePomFileForMavenPublication :client-api:generateMetadataFileForMavenPublication :client:jar :client:sourcesJar :client:generatePomFileForMavenPublication :client:generateMetadataFileForMavenPublication --rerun-tasks --no-daemon --console=plain`
- immutable bundle root: `gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16`
- `oxia-client-api-0.9.4.jar`: 38597 bytes, SHA-256 `fa2a973c19eafa83c7f2efb8d727d744b5405fb13e5a6adb9a92225f672455bf`
- `oxia-client-api-0.9.4-sources.jar`: 47855 bytes, SHA-256 `1a1e1d1125827c19b0733db84911ff7dbdd93aedb481880f1b85510640e8e6bb`
- `oxia-client-api-0.9.4.pom`: 4822 bytes, SHA-256 `1408ba3d6a9588303f0904e34329994a1c0e664210b3297966cb0a8b36930e77`
- `oxia-client-api-0.9.4.module`: 6036 bytes, SHA-256 `f4f2573b42dfd54ead0769cb990b32d8cf715ecc544d82d7c1b50e17573f5fec`
- `oxia-client-0.9.4.jar`: 385309 bytes, SHA-256 `0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5`
- `oxia-client-0.9.4-sources.jar`: 215856 bytes, SHA-256 `9dbfd9e9fafadc5415f1f6d53b0972f8acd3de5c8c957d4f96642e5e42e74a01`
- `oxia-client-0.9.4.pom`: 6875 bytes, SHA-256 `b48db12a661e7c4510a30cc816c6b19c5af623dbe5245f8fb8c34ff6afec8659`
- `oxia-client-0.9.4.module`: 7775 bytes, SHA-256 `1ac7c371b1bf0b7e571c597c09a1fe6acefe4e851892716b0b713061616d6d89`
- `manifest.sha256`: 1070 bytes, SHA-256 `521a7a3615b9f25d3e459633fff614f03208a13efda0ab9913b2255a9f2f40ab`

The same clean final commit rebuilt all eight module outputs twice with identical hashes. The client and client-api
POM/Gradle metadata bind `io.github.oxia-db:oxia-client-api:0.9.4` to the sibling artifact in the same bundle. The client
proto remained
byte-identical to the implementation base at SHA-256
`d2f3f4487eb28bc0c4bd40225ea49a875f8684bd35c477aa093229826b7d2ba2`.
