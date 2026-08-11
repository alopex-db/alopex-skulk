#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)}"
PARSER_ROOT="${ROOT_DIR}/nim-parser"
DESCRIPTOR="${PARSER_ROOT}/parser-consumer.json"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 1; }

mode="$(jq -er '.active_mode' "${DESCRIPTOR}")"
[[ "${mode}" == "public_release" ]] || {
  echo "parser consumer is not active in public_release mode: ${mode}" >&2
  exit 1
}

vendor_root="$(jq -er '.public_release.vendor_root' "${DESCRIPTOR}")"
contract_file="$(jq -er '.public_release.contract_file' "${DESCRIPTOR}")"
sums_file="$(jq -er '.public_release.sha256sums_file' "${DESCRIPTOR}")"
manifest_rel="$(jq -er '.public_release.manifest' "${DESCRIPTOR}")"
envelope_rel="$(jq -er '.public_release.envelope' "${DESCRIPTOR}")"
manifest="${PARSER_ROOT}/${manifest_rel}"
envelope="${PARSER_ROOT}/${envelope_rel}"

[[ -f "${manifest}" && -f "${envelope}" ]] || {
  echo "public manifest/envelope are missing" >&2
  exit 1
}

expected_version="$(jq -er '.public_release.alopex_version' "${DESCRIPTOR}")"
expected_contract="$(jq -er '.public_release.contract_version' "${DESCRIPTOR}")"
expected_tag="$(jq -er '.public_release.source.tag' "${DESCRIPTOR}")"
expected_tag_sha="$(jq -er '.public_release.source.tag_sha' "${DESCRIPTOR}")"
[[ "$(jq -er '.alopex_version' "${manifest}")" == "${expected_version}" ]]
[[ "$(jq -er '.contract_version' "${manifest}")" == "${expected_contract}" ]]
[[ "$(jq -er '.alopex_version' "${envelope}")" == "${expected_version}" ]]
[[ "$(jq -er '.contract_version' "${envelope}")" == "${expected_contract}" ]]
[[ "$(jq -er '.source.tag' "${envelope}")" == "${expected_tag}" ]]
[[ "$(jq -er '.source.tag_sha' "${envelope}")" == "${expected_tag_sha}" ]]

manifest_name="$(basename "${manifest}")"
manifest_sha="$(sha256sum "${manifest}" | awk '{print $1}')"
[[ "$(jq -er '.manifest.filename' "${envelope}")" == "${manifest_name}" ]]
[[ "$(jq -er '.manifest.sha256' "${envelope}")" == "${manifest_sha}" ]]
[[ "$(jq -er '.manifest.size' "${envelope}")" == "$(stat -c '%s' "${manifest}")" ]]

while IFS=$'\t' read -r target library_rel library_sha library_size archive_name archive_sha archive_size; do
  target_dir="${PARSER_ROOT}/${vendor_root}/${target}"
  library="${target_dir}/$(basename "${library_rel}")"
  [[ -f "${library}" ]] || { echo "missing ${target}: ${library}" >&2; exit 1; }
  [[ "$(cat "${target_dir}/${contract_file}")" == "${expected_contract}" ]]
  (cd "${target_dir}" && sha256sum -c "${sums_file}")
  [[ "$(sha256sum "${library}" | awk '{print $1}')" == "${library_sha}" ]]
  [[ "$(stat -c '%s' "${library}")" == "${library_size}" ]]
  envelope_asset="$(jq -er --arg target "${target}" '.assets[] | select(.target == $target)' "${envelope}")"
  [[ "$(jq -er '.filename' <<<"${envelope_asset}")" == "${archive_name}" ]]
  [[ "$(jq -er '.sha256' <<<"${envelope_asset}")" == "${archive_sha}" ]]
  [[ "$(jq -er '.size' <<<"${envelope_asset}")" == "${archive_size}" ]]
done < <(jq -r '.assets[] | [.target,.library.path,.library.sha256,(.library.size|tostring),.archive.filename,.archive.sha256,(.archive.size|tostring)] | @tsv' "${manifest}")

echo "parser consumer public release verified: ${expected_version}/${expected_contract}"
