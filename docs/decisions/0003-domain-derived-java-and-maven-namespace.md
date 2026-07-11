# ADR 0003: Use the Owned Domain for Java and Maven Namespaces

- Status: Accepted
- Date: 2026-07-11
- Scope: Repository-wide Java packages and published JVM coordinates

## Context

Phase 1 used `io.nereus.*` Java packages and the Maven group `io.nereusstream`. Neither namespace was
derived from a domain owned by the project. The project now owns `nereusstream.com`, the repository is still
`0.1.0-SNAPSHOT`, and no Git release tag exists. Keeping the placeholders would either imply control of
`nereus.io` / `nereusstream.io` or force a breaking migration after consumers exist.

## Decision

- Java package root: `com.nereusstream`.
- Maven group: `com.nereusstream`.
- Project URL in generated POMs: `https://nereusstream.com`.
- SCM coordinates remain `https://github.com/nereusstream/nereus`.
- `nereusGroup=com.nereusstream` in `gradle.properties` is the single build configuration source.
- No compatibility wrappers are retained for the pre-release `io.nereus.*` packages or
  `io.nereusstream` Maven group.

The migration changes source and artifact identity only. Oxia keys, deterministic ids, metadata codec type
ids/golden bytes, Object WAL bytes, and object-key layouts do not include Java package names and remain
unchanged.

## Consequences

- All modules, source sets, test fixtures, Docker-backed suites, resource paths, and code examples use
  `com.nereusstream.*`.
- Downstream source imports must use the new package root.
- A future namespace change requires a new ADR and an explicit compatibility plan.
- The migration is intentionally completed before the first tagged or stable artifact release.

## References

- `../../build.gradle.kts`
- `../../gradle.properties`
- `../phase-1-core-stream-storage/13-phase-1-final-review-2026-07-11.md`
