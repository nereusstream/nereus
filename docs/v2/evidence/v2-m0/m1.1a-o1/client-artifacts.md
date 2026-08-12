# M1.1a-O1 Oxia client artifact receipt

This is focused dependency evidence for source tuple `v2-m0`; it is not an M1 promotion receipt.

- repository: `https://github.com/nereusstream/oxia-client-java.git`
- implementation base: `24b730d1d66a1da701f4c99957361f6b3c5d748c`
- branch: `nereus/v2-m1.1a-o1-notification-continuity`
- final fork commit: `091a42c2780d92da56e9ec1f02ce1c3d988adc16`
- build: `./gradlew :client:jar :client:sourcesJar :client:generatePomFileForMavenPublication --rerun-tasks --no-daemon --console=plain`
- `client-0.9.4.jar`: 385309 bytes, SHA-256 `0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5`
- `client-0.9.4-sources.jar`: 215856 bytes, SHA-256 `9dbfd9e9fafadc5415f1f6d53b0972f8acd3de5c8c957d4f96642e5e42e74a01`
- `pom-default.xml`: 6875 bytes, SHA-256 `b48db12a661e7c4510a30cc816c6b19c5af623dbe5245f8fb8c34ff6afec8659`

The same clean final commit rebuilt all three outputs twice with identical hashes. The client proto remained
byte-identical to the implementation base at SHA-256
`d2f3f4487eb28bc0c4bd40225ea49a875f8684bd35c477aa093229826b7d2ba2`.
