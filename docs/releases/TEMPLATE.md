# Nereus vX.Y.Z release record

## Release identity

| Item | Exact value |
| --- | --- |
| Release commit | `FULL_SHA` |
| Annotated tag | `vX.Y.Z` |
| Gradle version | `X.Y.Z` |
| Release branch | `vX.Y` |
| Freeze timestamp (UTC) | `YYYY-MM-DDTHH:MM:SSZ` |

## External source locks

| Repository / component | Full commit or digest | Clean checkout verified by |
| --- | --- | --- |
| Pulsar | `FULL_SHA` | `COMMAND / RECEIPT` |
| Kafka | `FULL_SHA` | `COMMAND / RECEIPT` |
| Other runtime dependency | `FULL_SHA_OR_DIGEST` | `COMMAND / RECEIPT` |

## Compatibility decisions

- API changes:
- Configuration changes:
- Metadata/schema changes:
- Metrics/observability changes:
- Deployment or upgrade constraints:
- Release-only changes not applicable to `main`:

## Required gate evidence

| Gate | Result | Receipt / log path |
| --- | --- | --- |
| `./gradlew verifyReleaseVersion -PreleaseVersion=X.Y.Z` | `PASS` | `PATH` |
| Formatting and static checks | `PASS` | `PATH` |
| Full unit build | `PASS` | `PATH` |
| Source-locked final gates | `PASS` | `PATH` |

## Artifact identities

| Artifact / image | Immutable identity | Build manifest / checksum |
| --- | --- | --- |
| Maven artifacts | `COORDINATE + SHA256` | `PATH` |
| Runtime image | `REPOSITORY@sha256:DIGEST` | `PATH` |
| Admin image | `REPOSITORY@sha256:DIGEST` | `PATH` |

## Known limitations and follow-up

- Known limitations:
- Deferred work:
- Required `main` forward-ports:
- Support/branch-retention decision:
