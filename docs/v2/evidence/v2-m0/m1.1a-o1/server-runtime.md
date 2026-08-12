# M1.1a-O1 Oxia server runtime receipt

This is focused compatibility evidence for source tuple `v2-m0`; it does not authorize a server source change or an
M1 scenario promotion.

- source repository: `https://github.com/nereusstream/oxia.git`
- exact clean source commit before and after build/test: `37a17bef17202d5fd6e23282da5fd26d94865484`
- Docker build label `org.opencontainers.image.revision`: the same full source commit
- local image reference: `nereus/oxia-o1:37a17bef1720`
- image ID/digest: `sha256:5aa715e4f19091931743e5af489af5f8d6ee15efcce6430a908c6f65cc6d6516`
- platform: `linux/arm64`
- build command: `docker build --label org.opencontainers.image.revision=<server-source-commit> --label org.opencontainers.image.source=https://github.com/oxia-db/oxia.git -t nereus/oxia-o1:37a17bef1720 .`
- native notification check: `go test ./oxiad/dataserver/controller/lead -run '^TestLeaderController_Notifications' -count=1` passed

The server checkout stayed on `main`, remained clean, and received no branch, commit, push, proto edit, wire edit, or
RPC edit. The image reference is local evidence; it was not published to a registry.
