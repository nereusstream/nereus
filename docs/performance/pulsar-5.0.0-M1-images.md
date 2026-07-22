# Pulsar 5.0.0-M1 benchmark images

This note records the two image inputs and the containerd delivery paths for the first benchmark. The Helm values are
intentionally left for the cluster-specific follow-up.

## 1. Frozen inputs and worktrees

The local build uses two detached worktrees so neither image can accidentally consume files from the other branch:

| Variant | Worktree | HEAD |
| --- | --- | --- |
| Apache | `/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/pulsar-5.0.0-M1` | `8dae0236c0a0d405ed7f8303081080520fe91551` |
| Nereus | `/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/pulsar-5.0.0-M1-nereus` | `5ffc2caa0e08dac95bc8c2ea76ed3d32382dfe3e` |

Both worktrees report `version=5.0.0-M1`. The Nereus image also records Nereus source `81a1fa83e9aa4275229226cb895c72a6ea20ca87`,
runtime/test anchor `18cb06e8cd39454d8263b0134da980ad6aef6a31`, and adapter SHA-256
`7a913651ba3701bf0fce718d4fa7ef98b6767dfc5886cfa0865380756d0f7724`.

## 2. Locally built images

The first validation build targeted `linux/amd64` and completed on 2026-07-22:

| Variant | SHA-bearing tag | BuildKit-reported local image ID/digest | Size |
| --- | --- | --- | --- |
| Apache | `nereus-benchmark/pulsar:5.0.0-m1-apache-p8dae0236-amd64` | `sha256:c2d97a62bd34a9f5a8ca97a4b1b3ea27e670e881b27110940d2d4fe9c78a9c0e` | 489,662,785 bytes |
| Nereus | `nereus-benchmark/pulsar:5.0.0-m1-nereus-p5ffc2caa-n81a1fa8-amd64` | `sha256:04651c30a0b5711aa5a9ee2c693ace0b76539aeb737135106ca94948f6f63a49` | 567,396,852 bytes |

These are local image IDs/build digests, not registry `RepoDigest` values: an image does not acquire a registry digest until
it is pushed. Both images report Pulsar `5.0.0-M1`; the Apache image contains no `com.nereusstream` JARs, while the
Nereus image contains the expected eight `0.1.0-f2-dev` JARs.

The SHA-bearing tags prevent the two variants from sharing a mutable `latest` name, but tags can still be overwritten.
For deployment, retain the generated digest manifest as evidence and, when a registry is used, pin Helm to the pushed
`repository@sha256:...` value.

## 3. Build directly into containerd

The server build path uses `nerdctl`, not Docker. `nerdctl build` requires BuildKit (`buildkitd`) in addition to
containerd. Run the script from a clean Nereus `v0.1.0` checkout and point it at a Pulsar clone that contains both frozen
commits:

```bash
./scripts/build-pulsar-5.0.0-M1-images.sh \
  --pulsar-repo /srv/src/pulsar \
  --worktree-root /srv/src/pulsar-worktrees
```

If access to the containerd socket requires root, keep Gradle running as the normal user and elevate only nerdctl:

```bash
./scripts/build-pulsar-5.0.0-M1-images.sh \
  --pulsar-repo /srv/src/pulsar \
  --sudo-nerdctl
```

The script creates/verifies the two detached Pulsar worktrees, publishes the fixed Nereus development artifacts, builds
both distributions, builds both images in the `k8s.io` containerd namespace, runs smoke checks, and writes an env
manifest plus native inspect/digest output under `build/performance-images/`.

For a Kubernetes distribution with a non-default containerd socket, pass `--address /path/to/containerd.sock` to the
build script and set the same path through `CONTAINERD_ADDRESS` when saving or loading an archive.

A single-node cluster can use those local images immediately with `imagePullPolicy: Never`. Building on one node of a
multi-node cluster is not enough: another worker's containerd content store cannot see them.

## 4. Multi-node delivery

The preferred path is to build once and push both images to a private registry reachable by every worker:

```bash
./scripts/build-pulsar-5.0.0-M1-images.sh \
  --pulsar-repo /srv/src/pulsar \
  --image-repository registry.example.com/nereus/pulsar \
  --push
```

Record the registry digest emitted by the push/digest listing and deploy by digest. This avoids maintaining node-local
copies and allows normal Kubernetes scheduling.

If a registry is unavailable, create one dual Docker/OCI archive on the build node and import the exact same archive on
every eligible worker:

```bash
./scripts/containerd-transfer-pulsar-5.0.0-M1-images.sh save \
  build/performance-images/pulsar-5.0.0-M1-amd64.env \
  /srv/images/pulsar-5.0.0-M1-amd64.tar

CONTAINERD_USE_SUDO=true \
./scripts/containerd-transfer-pulsar-5.0.0-M1-images.sh load \
  /srv/images/pulsar-5.0.0-M1-amd64.tar \
  build/performance-images/pulsar-5.0.0-M1-amd64.env
```

The archive receives a `.sha256` sidecar and is verified before import. Loading prefers nerdctl's `load` command in the
`k8s.io` namespace and falls back to `ctr --namespace k8s.io images import`. With node-local imports, Helm must use the
exact tag and `imagePullPolicy: Never`, and the archive must be imported on every node that may run Pulsar Pods.

Rebuilding later from the same Git SHAs can still produce a different image digest because the upstream Pulsar Dockerfile
installs packages from live Alpine/Python repositories. Build once, then distribute that registry digest or that exact
archive for a single benchmark campaign.
