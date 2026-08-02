# Pulsar 5.0.0-M1 benchmark image workflow

This note defines how to build the two immutable Pulsar variants and the
supporting Nereus admin image for a containerd-backed Kubernetes campaign.

## 1. Source identity and worktrees

The build uses two detached Pulsar worktrees so neither variant can consume
files from the other branch:

| Variant | Ref | Required identity |
| --- | --- | --- |
| Apache Pulsar | `v5.0.0-M1` | `8dae0236c0a0d405ed7f8303081080520fe91551` |
| Nereus Pulsar | `5.0.0-M1-nereus` | `50fc70fe4620febcf0fd31d97ff7d2be447af3d4` |
| Nereus artifacts/admin | `v0.1.0` | final committed review tip |

Both Pulsar worktrees must report `version=5.0.0-M1`. Before the final image
build, commit in this order:

1. commit and validate the final `5.0.0-M1-nereus` Pulsar source;
2. set Nereus `gradle.properties:pulsarExpectedHead` to that full commit SHA
   and commit the final Nereus `v0.1.0` source;
3. from both clean commits, run `bookKeeperPrimaryWalFinalCheck`; if the gate
   requires any source change, commit it and rerun the complete gate;
4. run the image build from the exact two commits that passed.

The build script checks this source lock and rejects dirty/untracked files.
Until both review tips are committed, there is deliberately no final image
identity or digest to place in Helm.

## 2. Historical prototype images are not final

The 2026-07-22 validation build used the earlier integration anchors below:

| Variant | Historical tag | Historical local image ID |
| --- | --- | --- |
| Apache | `nereus-benchmark/pulsar:5.0.0-m1-apache-p8dae0236-amd64` | `sha256:c2d97a62bd34a9f5a8ca97a4b1b3ea27e670e881b27110940d2d4fe9c78a9c0e` |
| Nereus | `nereus-benchmark/pulsar:5.0.0-m1-nereus-p5ffc2caa-n81a1fa8-amd64` | `sha256:04651c30a0b5711aa5a9ee2c693ace0b76539aeb737135106ca94948f6f63a49` |

These images predate the admin CLI, Helm/runtime readiness review, and final
source commits. They are retained only as historical build evidence and must
not be used for the benchmark campaign.

The values above are local image IDs, not registry `RepoDigest` values. A
registry digest exists only after push. Tags are commit-qualified but can still
be overwritten; registry deployments must pin the recorded
`repository@sha256:...`.

## 3. Build directly into containerd

The server path uses `nerdctl`, not Docker. `nerdctl build` requires BuildKit
(`buildkitd`) in addition to containerd. Run from the clean final Nereus
`v0.1.0` checkout:

```bash
cd /root/denovo/nereus/nereus

./scripts/build-pulsar-5.0.0-M1-images.sh \
  --pulsar-repo /root/denovo/nereus/pulsar \
  --worktree-root /root/denovo/nereus/pulsar-worktrees \
  --nereus-pulsar-ref 5.0.0-M1-nereus \
  --admin-base-image 'eclipse-temurin:21-jre-noble@sha256:<PINNED_DIGEST>'
```

If access to the containerd socket requires root, keep Gradle running as the
normal user and elevate only nerdctl:

```bash
./scripts/build-pulsar-5.0.0-M1-images.sh \
  --pulsar-repo /root/denovo/nereus/pulsar \
  --admin-base-image 'eclipse-temurin:21-jre-noble@sha256:<PINNED_DIGEST>' \
  --sudo-nerdctl
```

When the build host reaches external sources through a proxy, export the
proxy variables before invoking the script. The script forwards the set
uppercase and lowercase variables as Docker build arguments so native
dependency steps such as the Snappy build use the same route:

```bash
export HTTP_PROXY=http://<build-host-reachable-proxy>:<port>
export HTTPS_PROXY="${HTTP_PROXY}"
export NO_PROXY=localhost,127.0.0.1
```

The script creates/verifies the two detached Pulsar worktrees, publishes Nereus
artifacts against the exact selected fork commit, builds both Pulsar
distributions, and builds:

- Apache Pulsar, tagged with the Apache commit;
- Nereus Pulsar, tagged with both Pulsar and Nereus commits;
- Nereus admin, tagged with the Nereus commit.

All three local images must pass their offline smoke checks before `--push`
publishes anything. The admin Dockerfile has no mutable default base image; the
authoritative script must supply the required digest-pinned JRE 21 image. The
script then writes image IDs, native inspect output, digest listings, all three
Dockerfile SHA-256 identities, and a checksummed env manifest under
`build/performance-images/`.

For a non-default containerd socket, pass
`--address /path/to/containerd.sock` and use the same
`CONTAINERD_ADDRESS` when saving or loading the archive.

A single-node cluster can use local images with `imagePullPolicy: Never`.
Building on one node of a multi-node cluster is not enough: another worker's
containerd content store cannot see them.

## 4. Multi-node delivery

The preferred path is to build once and push all three images to a private
registry reachable by every worker:

```bash
./scripts/build-pulsar-5.0.0-M1-images.sh \
  --pulsar-repo /root/denovo/nereus/pulsar \
  --image-repository registry.example.com/nereus/pulsar \
  --admin-image-repository registry.example.com/nereus/nereus-admin \
  --admin-base-image 'eclipse-temurin:21-jre-noble@sha256:<PINNED_DIGEST>' \
  --push
```

Record each registry digest emitted by the push/digest listing and deploy by
digest. This avoids maintaining node-local copies and allows normal Kubernetes
scheduling.

If a registry is unavailable, create one archive on the build node and import
the exact same archive on every eligible worker:

```bash
./scripts/containerd-transfer-pulsar-5.0.0-M1-images.sh save \
  build/performance-images/pulsar-5.0.0-M1-amd64.env \
  /srv/images/pulsar-5.0.0-M1-amd64.tar

CONTAINERD_USE_SUDO=true \
./scripts/containerd-transfer-pulsar-5.0.0-M1-images.sh load \
  /srv/images/pulsar-5.0.0-M1-amd64.tar \
  build/performance-images/pulsar-5.0.0-M1-amd64.env
```

The archive and env manifest each require their `.sha256` sidecar and are
verified before import.
Loading prefers `nerdctl load` in the `k8s.io` namespace and falls back to
`ctr --namespace k8s.io images import`. With node-local imports, Helm must use
the exact tags and `imagePullPolicy: Never`, and the archive must be imported
on every node that may run these Pods.

Rebuilding later from the same Git SHAs can still produce a different image
digest because the upstream Pulsar Dockerfile installs packages from live
repositories. Pin every available base image by digest, build once, and
distribute that registry digest or the exact checksummed archive for one
campaign.
