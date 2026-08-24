# syntax=docker/dockerfile:1.7

# The M1 local-only image used moving base tags and was never published, so its historical config digest cannot be
# reconstructed after the local image is lost. M3 uses this independently versioned, pinned and timestamp-normalized
# evidence image. The Oxia source remains the accepted O1 commit; this recipe changes no Oxia product source.
FROM golang:1.26-alpine@sha256:28d89ee9cc0ff9fec75c82ca201e6bf7fdf9a679d4b7b24dfa04f2bb766bb468 AS build

ARG OXIA_VERSION
ARG SOURCE_DATE_EPOCH
WORKDIR /src/oxia
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test "$OXIA_VERSION" = "0.16.3-167-g37a17bef" && \
    test "$SOURCE_DATE_EPOCH" = "1786412361" && \
    CGO_ENABLED=0 go build \
        -buildvcs=false \
        -tags disable_trap \
        -trimpath \
        -ldflags "-buildid= -X main.version=$OXIA_VERSION" \
        -o /out/oxia ./cmd && \
    touch -d "@$SOURCE_DATE_EPOCH" \
        /out/oxia \
        /src/oxia/conf/coordinator.yaml \
        /src/oxia/conf/dataserver.yaml

FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

ARG OXIA_SOURCE_COMMIT
LABEL org.opencontainers.image.revision="$OXIA_SOURCE_COMMIT"
LABEL org.opencontainers.image.source="https://github.com/oxia-db/oxia.git"
LABEL com.nereusstream.evidence.recipe="NEREUS_V2_M3_ALLOCATOR_OXIA_IMAGE_V1"

WORKDIR /oxia
COPY --from=build /out/oxia /oxia/bin/oxia
COPY --from=build /src/oxia/conf/coordinator.yaml /oxia/conf/coordinator.yaml
COPY --from=build /src/oxia/conf/dataserver.yaml /oxia/conf/dataserver.yaml
ENV PATH="/oxia/bin:$PATH"
CMD ["/bin/sh"]
