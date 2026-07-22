#!/usr/bin/env bash

set -euo pipefail

containerd_namespace="${CONTAINERD_NAMESPACE:-k8s.io}"
containerd_address="${CONTAINERD_ADDRESS:-}"
nerdctl_bin="${NERDCTL_BIN:-nerdctl}"
ctr_bin="${CTR_BIN:-ctr}"
use_sudo="${CONTAINERD_USE_SUDO:-false}"

usage() {
    cat <<'EOF'
Save or load the two Pulsar benchmark images for a containerd-backed Kubernetes cluster.

Usage:
  containerd-transfer-pulsar-5.0.0-M1-images.sh save MANIFEST.env IMAGES.tar
  containerd-transfer-pulsar-5.0.0-M1-images.sh load IMAGES.tar [MANIFEST.env]

Environment:
  CONTAINERD_NAMESPACE   containerd namespace (default: k8s.io)
  CONTAINERD_ADDRESS     containerd socket/address (default: CLI default)
  CONTAINERD_USE_SUDO    true to run nerdctl/ctr through sudo (default: false)
  NERDCTL_BIN            nerdctl executable (default: nerdctl)
  CTR_BIN                ctr fallback executable for load (default: ctr)

The save command requires nerdctl. The load command prefers nerdctl and falls back
to `ctr --namespace k8s.io images import`. Run load on every Kubernetes node that
may schedule a Pod using locally imported images.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        die "sha256sum or shasum is required"
    fi
}

manifest_value() {
    local manifest="$1"
    local key="$2"

    sed -n "s/^${key}=//p" "${manifest}" | tail -n 1
}

run_nerdctl() {
    local -a global_args=(--namespace "${containerd_namespace}")
    if [[ -n "${containerd_address}" ]]; then
        global_args+=(--address "${containerd_address}")
    fi
    if [[ "${use_sudo}" == true ]]; then
        sudo "${nerdctl_bin}" "${global_args[@]}" "$@"
    else
        "${nerdctl_bin}" "${global_args[@]}" "$@"
    fi
}

run_ctr() {
    local -a global_args=(--namespace "${containerd_namespace}")
    if [[ -n "${containerd_address}" ]]; then
        global_args+=(--address "${containerd_address}")
    fi
    if [[ "${use_sudo}" == true ]]; then
        sudo "${ctr_bin}" "${global_args[@]}" "$@"
    else
        "${ctr_bin}" "${global_args[@]}" "$@"
    fi
}

[[ $# -ge 1 ]] || {
    usage
    exit 1
}

command_name="$1"
shift

case "${command_name}" in
    save)
        [[ $# -eq 2 ]] || die "save requires MANIFEST.env and IMAGES.tar"
        manifest_file="$1"
        archive_file="$2"
        [[ -f "${manifest_file}" ]] || die "manifest not found: ${manifest_file}"
        [[ ! -e "${archive_file}" ]] || die "refusing to overwrite archive: ${archive_file}"
        [[ ! -e "${archive_file}.sha256" ]] || die "refusing to overwrite checksum: ${archive_file}.sha256"
        command -v "${nerdctl_bin}" >/dev/null 2>&1 || die "save requires nerdctl: ${nerdctl_bin}"
        if [[ "${use_sudo}" == true ]]; then
            command -v sudo >/dev/null 2>&1 || die "sudo is required when CONTAINERD_USE_SUDO=true"
        fi

        apache_image="$(manifest_value "${manifest_file}" APACHE_IMAGE)"
        nereus_image="$(manifest_value "${manifest_file}" NEREUS_IMAGE)"
        target_platform="$(manifest_value "${manifest_file}" TARGET_PLATFORM)"
        [[ -n "${apache_image}" ]] || die "APACHE_IMAGE is missing from ${manifest_file}"
        [[ -n "${nereus_image}" ]] || die "NEREUS_IMAGE is missing from ${manifest_file}"
        [[ -n "${target_platform}" ]] || die "TARGET_PLATFORM is missing from ${manifest_file}"

        mkdir -p "$(dirname "${archive_file}")"
        run_nerdctl save --platform "${target_platform}" --output "${archive_file}" \
            "${apache_image}" "${nereus_image}"
        archive_sha256="$(sha256_file "${archive_file}")"
        printf '%s  %s\n' "${archive_sha256}" "$(basename "${archive_file}")" \
            >"${archive_file}.sha256"
        echo "saved ${archive_file}"
        echo "archive sha256: ${archive_sha256}"
        ;;
    load)
        [[ $# -ge 1 && $# -le 2 ]] || die "load requires IMAGES.tar and optional MANIFEST.env"
        archive_file="$1"
        manifest_file="${2:-}"
        [[ -f "${archive_file}" ]] || die "archive not found: ${archive_file}"
        if [[ -f "${archive_file}.sha256" ]]; then
            expected_sha256="$(awk 'NR == 1 {print $1}' "${archive_file}.sha256")"
            actual_sha256="$(sha256_file "${archive_file}")"
            [[ "${actual_sha256}" == "${expected_sha256}" ]] \
                || die "archive checksum mismatch: expected ${expected_sha256}, got ${actual_sha256}"
            echo "verified archive sha256: ${actual_sha256}"
        else
            echo "warning: checksum sidecar not found: ${archive_file}.sha256" >&2
        fi
        if [[ "${use_sudo}" == true ]]; then
            command -v sudo >/dev/null 2>&1 || die "sudo is required when CONTAINERD_USE_SUDO=true"
        fi

        if command -v "${nerdctl_bin}" >/dev/null 2>&1; then
            run_nerdctl load --input "${archive_file}"
        elif command -v "${ctr_bin}" >/dev/null 2>&1; then
            run_ctr images import "${archive_file}"
        else
            die "neither nerdctl nor ctr is available"
        fi

        if [[ -n "${manifest_file}" ]]; then
            [[ -f "${manifest_file}" ]] || die "manifest not found: ${manifest_file}"
            apache_image="$(manifest_value "${manifest_file}" APACHE_IMAGE)"
            nereus_image="$(manifest_value "${manifest_file}" NEREUS_IMAGE)"
            if command -v "${nerdctl_bin}" >/dev/null 2>&1; then
                run_nerdctl images --digests --no-trunc "${apache_image}"
                run_nerdctl images --digests --no-trunc "${nereus_image}"
            else
                run_ctr images list | grep -F "${apache_image}"
                run_ctr images list | grep -F "${nereus_image}"
            fi
        fi
        echo "loaded images into containerd namespace ${containerd_namespace}"
        ;;
    -h | --help | help)
        usage
        ;;
    *)
        die "unknown command: ${command_name}"
        ;;
esac
