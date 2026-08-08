# Nereus release freeze and tag process

This document defines the required process for freezing a Nereus release commit and creating an immutable Git tag.
Release branches are temporary stabilization lines; `main` remains the owner of reusable product, test, admin, and
build capabilities. Deleting a release branch is a separate decision and is never part of this procedure.

## 1. Naming and version authority

- Use `release/<major>.<minor>` for a stabilization branch, for example `release/0.2`.
- Use `v<major>.<minor>.<patch>` for the immutable annotated tag, for example `v0.2.0`.
- `gradle.properties:nereusVersion` is the single product-version authority.
- During normal development it is `X.Y.Z-SNAPSHOT`; the release-freeze commit changes it to the exact `X.Y.Z`.
- Phase 2 and Phase 9 development coordinates are derived automatically as `X.Y.Z-f2-dev` and `X.Y.Z-f9-dev`.
- Dockerfiles consume an explicit version build argument. They must not contain a release-number default.

Historical documentation, golden vectors, compatibility fixtures, and version-specific reconstruction scripts may
legitimately contain older versions. Never perform a repository-wide version replacement.

## 2. Files reviewed for every release

The freeze owner must review and, where applicable, update:

1. `gradle.properties`
   - set `nereusVersion=X.Y.Z`;
   - pin `pulsarExpectedHead` when the release consumes a release-specific Pulsar fork.
2. `.github/workflows/build.yml`
   - verify every external checkout `ref` matches the source baseline used by the final gates.
3. Release-specific build scripts and manifests
   - add a new script/manifest for the new baseline; do not rewrite a historical release reconstruction script;
   - pin source refs and base images, and keep commit-qualified image names.
4. `docs/releases/vX.Y.Z.md`
   - instantiate [`TEMPLATE.md`](TEMPLATE.md) and record source locks, gates, artifacts, compatibility decisions, and
     known limitations.
5. User-facing API, schema, configuration, metric, and deployment documentation affected by the release.

Use this audit to find candidates, then classify every result instead of replacing it blindly:

```bash
rg -n '0\\.1\\.0|v0\\.1\\.0|SNAPSHOT|ExpectedHead|EXPECTED_HEAD|^[[:space:]]*ref:' \
  gradle.properties build.gradle.kts .github scripts docker docs
```

## 3. Freeze workflow

Start only from a clean, current `main` and cut the stabilization branch late:

```bash
git fetch origin --prune --tags
git switch main
git pull --ff-only origin main
git switch -c release/0.2
```

Complete all release changes before the freeze commit:

1. Change `gradle.properties:nereusVersion` from `0.2.0-SNAPSHOT` to `0.2.0`.
2. Add `docs/releases/v0.2.0.md` from the template and finalize every source lock.
3. Run formatting, static checks, and unit tests against the candidate tree.
4. Stage only the reviewed release paths and create one explicit freeze commit:

```bash
git commit -m '[release][0.2.0] Freeze v0.2.0'
```

From that clean freeze commit:

1. Run the version gate:

   ```bash
   ./gradlew verifyReleaseVersion -PreleaseVersion=0.2.0
   ```

2. Run the full build and every source-locked final gate claimed by the release.
3. Build release artifacts/images and record checksums and registry digests.
4. Re-run any gate affected by a source or documentation correction.

The freeze commit must be clean and must itself be the source used for the final artifact build. Evidence collected
from a parent commit does not qualify the freeze commit. If validation requires a correction, do not tag the failed
candidate. Recreate the unpublished freeze commit and rerun the complete freeze-commit validation.

## 4. Tag and publication

After the freeze commit and its required CI/final gates pass:

```bash
release_sha="$(git rev-parse HEAD)"
git tag -a v0.2.0 "${release_sha}" -m 'Nereus v0.2.0'
test "$(git rev-list -n 1 v0.2.0)" = "${release_sha}"
git push origin release/0.2
git push origin refs/tags/v0.2.0
```

Verify the remote tag and reconstruct the release from a fresh detached checkout. The reconstruction must not depend
on a local branch name. Never move or replace a published release tag; issue `v0.2.1` for a correction.

Branch retention or deletion happens only after artifact, deployment, and support owners explicitly decide it. A tag
does not imply automatic branch deletion.

## 5. Return to development

If `main` has not already advanced, bump its next development version in a separate commit, for example
`nereusVersion=0.3.0-SNAPSHOT`. Product fixes should normally land on `main` first and then be cherry-picked to the
active release branch. A truly release-only compatibility fix must record why it is not applicable to `main`.
