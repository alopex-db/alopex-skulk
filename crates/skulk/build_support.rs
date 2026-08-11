use sha2::Digest;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
pub(crate) const NIM_PARSER_CONTRACT_VERSION: &str = "0.2.0";

#[allow(dead_code)]
pub(crate) const VENDORED_TARGETS: [&str; 4] = [
    "x86_64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
];

pub(crate) fn nim_lib_filename_for(target_os: &str) -> Result<&'static str, String> {
    match target_os {
        "linux" => Ok("libalopex_sql_parser.so"),
        "macos" => Ok("libalopex_sql_parser.dylib"),
        "windows" => Ok("alopex_sql_parser.dll"),
        unsupported => Err(format!(
            "the Nim query parser does not support target OS `{unsupported}`"
        )),
    }
}

#[allow(dead_code)]
pub(crate) fn is_vendored_target(target: &str) -> bool {
    VENDORED_TARGETS.contains(&target)
}

#[allow(dead_code)]
pub(crate) fn resolve_library_dir(
    manifest_dir: &Path,
    target: &str,
    filename: &str,
    override_dir: Option<&Path>,
) -> Result<Option<PathBuf>, String> {
    if let Some(dir) = override_dir {
        let library = dir.join(filename);
        return library
            .is_file()
            .then(|| dir.to_path_buf())
            .map(Some)
            .ok_or_else(|| {
                format!(
                    "SKULK_NIM_PARSER_LIB_DIR points to `{}`, but `{}` does not exist",
                    dir.display(),
                    library.display()
                )
            });
    }

    if !is_vendored_target(target) {
        return Ok(None);
    }

    let vendored = manifest_dir.join("nim-parser/vendor").join(target);
    Ok(vendored.join(filename).is_file().then_some(vendored))
}

#[allow(dead_code)]
pub(crate) fn validate_contract_version_file(library_dir: &Path) -> Result<(), String> {
    let path = library_dir.join("CONTRACT_VERSION");
    let actual = fs::read_to_string(&path).map_err(|error| {
        format!(
            "failed to read Nim parser contract version `{}`: {error}",
            path.display()
        )
    })?;
    let actual = actual.trim();
    if actual == NIM_PARSER_CONTRACT_VERSION {
        Ok(())
    } else {
        Err(format!(
            "Nim parser artifact `{}` declares contract `{actual}`, but Skulk supports `{NIM_PARSER_CONTRACT_VERSION}`",
            library_dir.display()
        ))
    }
}

/// Parsed but inactive parser-consumer configuration. Task 2.0.0 prepares
/// this API; the current build script intentionally continues to use the
/// legacy resolver above until the activation task is complete.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ParserConsumerDescriptor {
    value: serde_json::Value,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParserTargetResolution {
    pub(crate) mode: String,
    pub(crate) target: String,
    pub(crate) library_path: PathBuf,
    pub(crate) contract_version: String,
    pub(crate) source_ref: String,
    pub(crate) manifest_path: Option<PathBuf>,
    pub(crate) envelope_path: Option<PathBuf>,
}

#[allow(dead_code)]
pub(crate) fn load_parser_consumer_descriptor(
    path: &Path,
) -> Result<ParserConsumerDescriptor, String> {
    let bytes = fs::read(path).map_err(|error| {
        format!(
            "failed to read parser consumer descriptor `{}`: {error}",
            path.display()
        )
    })?;
    let value = serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "failed to parse parser consumer descriptor `{}`: {error}",
            path.display()
        )
    })?;
    validate_parser_consumer_descriptor(&value)?;
    Ok(ParserConsumerDescriptor { value })
}

#[allow(dead_code)]
pub(crate) fn validate_parser_consumer_descriptor(value: &serde_json::Value) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| "parser consumer descriptor must be a JSON object".to_string())?;
    if required_string(object, "schema")? != "skulk-parser-consumer-v1" {
        return Err("parser consumer descriptor has an unsupported schema".to_string());
    }
    if required_string(object, "active_mode")? != "legacy" {
        return Err("parser consumer descriptor must remain inactive in legacy mode".to_string());
    }

    let legacy = required_object(object, "legacy")?;
    for key in [
        "source_repository",
        "source_ref",
        "contract_version",
        "vendor_root",
        "contract_file",
        "sha256sums_file",
    ] {
        required_string(legacy, key)?;
    }
    validate_relative_path(legacy, "vendor_root")?;
    validate_relative_filename(legacy, "contract_file")?;
    validate_relative_filename(legacy, "sha256sums_file")?;

    let public = required_object(object, "public_release")?;
    for key in [
        "alopex_version",
        "contract_version",
        "vendor_root",
        "contract_file",
        "sha256sums_file",
        "manifest",
        "envelope",
    ] {
        required_string(public, key)
            .map_err(|_| format!("public_release.{key} must be a non-empty string"))?;
    }
    validate_relative_path(public, "vendor_root")?;
    validate_relative_path(public, "manifest")?;
    validate_relative_path(public, "envelope")?;
    let source = required_object(public, "source")
        .map_err(|_| "public_release.source must be an object".to_string())?;
    required_string(source, "tag")
        .map_err(|_| "public_release.source.tag must be a non-empty string".to_string())?;
    let tag_sha = required_string(source, "tag_sha")
        .map_err(|_| "public_release.source.tag_sha must be a non-empty string".to_string())?;
    if tag_sha.len() != 40 || !tag_sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("public_release.source.tag_sha must be a 40-character hex SHA".to_string());
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn resolve_parser_target(
    descriptor: &ParserConsumerDescriptor,
    mode: &str,
    target: &str,
    root: &Path,
) -> Result<ParserTargetResolution, String> {
    resolve_parser_target_with_override(descriptor, mode, target, root, None)
}

#[allow(dead_code)]
pub(crate) fn resolve_parser_target_with_override(
    descriptor: &ParserConsumerDescriptor,
    mode: &str,
    target: &str,
    root: &Path,
    override_dir: Option<&Path>,
) -> Result<ParserTargetResolution, String> {
    let object = descriptor
        .value
        .as_object()
        .ok_or_else(|| "parser consumer descriptor must be a JSON object".to_string())?;
    let mode_object = required_object(object, mode_for_key(mode)?)?;
    let target_root = if let Some(dir) = override_dir {
        dir.to_path_buf()
    } else {
        root.join(required_string(mode_object, "vendor_root")?)
            .join(target)
    };
    let target_os = if target.ends_with("-windows-msvc") {
        "windows"
    } else if target.ends_with("-apple-darwin") {
        "macos"
    } else if target.ends_with("-unknown-linux-gnu") {
        "linux"
    } else {
        return Err(format!("unsupported parser target `{target}`"));
    };
    let library_path = target_root.join(nim_lib_filename_for(target_os)?);
    if !library_path.is_file() {
        return Err(format!(
            "parser target `{target}` is missing library `{}`",
            library_path.display()
        ));
    }
    let contract_file = target_root.join(required_string(mode_object, "contract_file")?);
    let contract_version = required_string(mode_object, "contract_version")?.to_string();
    let actual_contract = fs::read_to_string(&contract_file)
        .map_err(|error| format!("failed to read `{}`: {error}", contract_file.display()))?;
    if actual_contract.trim() != contract_version {
        return Err(format!(
            "parser target `{target}` declares contract `{}`, expected `{contract_version}`",
            actual_contract.trim()
        ));
    }
    let sums_file = target_root.join(required_string(mode_object, "sha256sums_file")?);
    let sums = fs::read_to_string(&sums_file)
        .map_err(|error| format!("failed to read `{}`: {error}", sums_file.display()))?;
    let expected_digest = hex_digest(
        &fs::read(&library_path)
            .map_err(|error| format!("failed to read `{}`: {error}", library_path.display()))?,
    );
    let library_name = library_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "parser library filename is not valid UTF-8".to_string())?;
    if !sums.lines().any(|line| {
        let mut fields = line.split_whitespace();
        fields.next() == Some(expected_digest.as_str()) && fields.next() == Some(library_name)
    }) {
        return Err(format!(
            "SHA256SUMS for `{target}` does not bind `{library_name}`"
        ));
    }

    let (source_ref, manifest_path, envelope_path) = if mode == "legacy" {
        (
            required_string(mode_object, "source_ref")?.to_string(),
            None,
            None,
        )
    } else {
        let manifest_path = root.join(required_string(mode_object, "manifest")?);
        let envelope_path = root.join(required_string(mode_object, "envelope")?);
        validate_public_release_identity(
            mode_object,
            target,
            &manifest_path,
            &envelope_path,
            &library_path,
            &expected_digest,
        )?;
        (
            required_string(required_object(mode_object, "source")?, "tag")?.to_string(),
            Some(manifest_path),
            Some(envelope_path),
        )
    };

    Ok(ParserTargetResolution {
        mode: mode.to_string(),
        target: target.to_string(),
        library_path,
        contract_version,
        source_ref,
        manifest_path,
        envelope_path,
    })
}

#[allow(dead_code)]
fn validate_public_release_identity(
    descriptor: &serde_json::Map<String, serde_json::Value>,
    target: &str,
    manifest_path: &Path,
    envelope_path: &Path,
    library_path: &Path,
    library_digest: &str,
) -> Result<(), String> {
    let manifest_bytes = fs::read(manifest_path).map_err(|error| {
        format!(
            "public release manifest `{}` is required: {error}",
            manifest_path.display()
        )
    })?;
    let envelope_bytes = fs::read(envelope_path).map_err(|error| {
        format!(
            "public release envelope `{}` is required: {error}",
            envelope_path.display()
        )
    })?;
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| format!("invalid public release manifest: {error}"))?;
    let envelope: serde_json::Value = serde_json::from_slice(&envelope_bytes)
        .map_err(|error| format!("invalid public release envelope: {error}"))?;
    let expected_version = required_string(descriptor, "alopex_version")?;
    let expected_contract = required_string(descriptor, "contract_version")?;
    if manifest
        .get("alopex_version")
        .and_then(|value| value.as_str())
        != Some(expected_version)
        || manifest
            .get("contract_version")
            .and_then(|value| value.as_str())
            != Some(expected_contract)
    {
        return Err("public release manifest does not match descriptor identity".to_string());
    }
    if envelope
        .get("alopex_version")
        .and_then(|value| value.as_str())
        != Some(expected_version)
        || envelope
            .get("contract_version")
            .and_then(|value| value.as_str())
            != Some(expected_contract)
    {
        return Err("public release envelope does not match descriptor identity".to_string());
    }
    let expected_source = required_object(descriptor, "source")?;
    let envelope_source = envelope
        .get("source")
        .and_then(|value| value.as_object())
        .ok_or_else(|| "public release envelope is missing source identity".to_string())?;
    if envelope_source.get("tag").and_then(|value| value.as_str())
        != expected_source.get("tag").and_then(|value| value.as_str())
        || envelope_source
            .get("tag_sha")
            .and_then(|value| value.as_str())
            != expected_source
                .get("tag_sha")
                .and_then(|value| value.as_str())
    {
        return Err(
            "public release envelope source identity does not match descriptor".to_string(),
        );
    }
    let target_record = manifest
        .get("assets")
        .and_then(|value| value.as_array())
        .and_then(|assets| {
            assets
                .iter()
                .find(|asset| asset.get("target").and_then(|value| value.as_str()) == Some(target))
        })
        .ok_or_else(|| format!("public release manifest has no target `{target}`"))?;
    let library_record = target_record
        .get("library")
        .and_then(|value| value.as_object())
        .ok_or_else(|| format!("public release manifest has no library for `{target}`"))?;
    if library_record
        .get("sha256")
        .and_then(|value| value.as_str())
        != Some(library_digest)
    {
        return Err(format!(
            "public release manifest library digest does not match `{target}`"
        ));
    }
    let library_size = fs::metadata(library_path)
        .map_err(|error| format!("failed to stat `{}`: {error}", library_path.display()))?
        .len();
    if library_record.get("size").and_then(|value| value.as_u64()) != Some(library_size) {
        return Err(format!(
            "public release manifest library size does not match `{target}`"
        ));
    }
    let envelope_manifest = envelope
        .get("manifest")
        .and_then(|value| value.as_object())
        .ok_or_else(|| "public release envelope is missing manifest binding".to_string())?;
    let manifest_name = manifest_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "public release manifest filename is not valid UTF-8".to_string())?;
    let manifest_digest = hex_digest(&manifest_bytes);
    if envelope_manifest
        .get("filename")
        .and_then(|value| value.as_str())
        != Some(manifest_name)
        || envelope_manifest
            .get("sha256")
            .and_then(|value| value.as_str())
            != Some(manifest_digest.as_str())
        || envelope_manifest
            .get("size")
            .and_then(|value| value.as_u64())
            != Some(manifest_bytes.len() as u64)
    {
        return Err(
            "public release envelope manifest binding does not match local manifest".to_string(),
        );
    }
    let envelope_asset = envelope
        .get("assets")
        .and_then(|value| value.as_array())
        .and_then(|assets| {
            assets
                .iter()
                .find(|asset| asset.get("target").and_then(|value| value.as_str()) == Some(target))
        })
        .ok_or_else(|| format!("public release envelope has no target `{target}`"))?;
    let archive_record = target_record
        .get("archive")
        .and_then(|value| value.as_object())
        .ok_or_else(|| format!("public release manifest has no archive for `{target}`"))?;
    if envelope_asset.get("filename") != archive_record.get("filename")
        || envelope_asset.get("sha256") != archive_record.get("sha256")
        || envelope_asset.get("size") != archive_record.get("size")
    {
        return Err(format!(
            "public release envelope archive binding does not match `{target}`"
        ));
    }
    if target_record.get("target").and_then(|value| value.as_str()) != Some(target) {
        return Err(format!(
            "public release target identity mismatch for `{target}`"
        ));
    }
    Ok(())
}

fn mode_for_key(mode: &str) -> Result<&'static str, String> {
    match mode {
        "legacy" => Ok("legacy"),
        "public_release" => Ok("public_release"),
        unsupported => Err(format!("unsupported parser consumer mode `{unsupported}`")),
    }
}

fn required_object<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<&'a serde_json::Map<String, serde_json::Value>, String> {
    object
        .get(key)
        .and_then(|value| value.as_object())
        .ok_or_else(|| format!("parser consumer descriptor requires object `{key}`"))
}

fn required_string<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<&'a str, String> {
    object
        .get(key)
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("parser consumer descriptor requires non-empty string `{key}`"))
}

fn validate_relative_path(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<(), String> {
    let value = required_string(object, key)?;
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| component == std::path::Component::ParentDir)
    {
        return Err(format!(
            "parser consumer descriptor path `{key}` must be relative and contained"
        ));
    }
    Ok(())
}

fn validate_relative_filename(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<(), String> {
    validate_relative_path(object, key)?;
    let value = required_string(object, key)?;
    if value.contains('/') || value.contains('\\') {
        return Err(format!(
            "parser consumer descriptor filename `{key}` must be a basename"
        ));
    }
    Ok(())
}

fn hex_digest(bytes: &[u8]) -> String {
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;
    use std::fs;
    use std::path::PathBuf;

    fn scratch_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "alopex-skulk-build-support-{}-{name}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn target_os_selects_the_shared_library_filename() {
        assert_eq!(
            nim_lib_filename_for("linux").unwrap(),
            "libalopex_sql_parser.so"
        );
        assert_eq!(
            nim_lib_filename_for("macos").unwrap(),
            "libalopex_sql_parser.dylib"
        );
        assert_eq!(
            nim_lib_filename_for("windows").unwrap(),
            "alopex_sql_parser.dll"
        );
        assert!(nim_lib_filename_for("freebsd").is_err());
    }

    #[test]
    fn vendored_targets_match_the_release_matrix() {
        assert!(is_vendored_target("x86_64-unknown-linux-gnu"));
        assert!(is_vendored_target("x86_64-apple-darwin"));
        assert!(is_vendored_target("aarch64-apple-darwin"));
        assert!(is_vendored_target("x86_64-pc-windows-msvc"));
        assert!(!is_vendored_target("aarch64-unknown-linux-gnu"));
    }

    #[test]
    fn explicit_library_directory_precedes_the_vendored_artifact() {
        let root = scratch_dir("override");
        let override_dir = root.join("override");
        let vendored_dir = root.join("nim-parser/vendor/x86_64-unknown-linux-gnu");
        fs::create_dir_all(&override_dir).unwrap();
        fs::create_dir_all(&vendored_dir).unwrap();
        fs::write(override_dir.join("libalopex_sql_parser.so"), []).unwrap();
        fs::write(vendored_dir.join("libalopex_sql_parser.so"), []).unwrap();

        assert_eq!(
            resolve_library_dir(
                &root,
                "x86_64-unknown-linux-gnu",
                "libalopex_sql_parser.so",
                Some(&override_dir),
            )
            .unwrap(),
            Some(override_dir)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_invalid_explicit_library_directory_is_an_error() {
        let root = scratch_dir("missing-override");
        let override_dir = root.join("missing");

        let error = resolve_library_dir(
            &root,
            "x86_64-unknown-linux-gnu",
            "libalopex_sql_parser.so",
            Some(&override_dir),
        )
        .unwrap_err();

        assert!(error.contains("SKULK_NIM_PARSER_LIB_DIR"));
        assert!(error.contains("libalopex_sql_parser.so"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn vendored_resolution_requires_a_supported_target_and_existing_artifact() {
        let root = scratch_dir("vendored");
        let supported_dir = root.join("nim-parser/vendor/x86_64-unknown-linux-gnu");
        fs::create_dir_all(&supported_dir).unwrap();
        fs::write(supported_dir.join("libalopex_sql_parser.so"), []).unwrap();

        assert_eq!(
            resolve_library_dir(
                &root,
                "x86_64-unknown-linux-gnu",
                "libalopex_sql_parser.so",
                None,
            )
            .unwrap(),
            Some(supported_dir)
        );
        assert_eq!(
            resolve_library_dir(
                &root,
                "aarch64-unknown-linux-gnu",
                "libalopex_sql_parser.so",
                None,
            )
            .unwrap(),
            None
        );
        assert_eq!(
            resolve_library_dir(
                &root,
                "x86_64-apple-darwin",
                "libalopex_sql_parser.dylib",
                None,
            )
            .unwrap(),
            None
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn contract_version_file_must_exist_and_match_the_supported_version() {
        let root = scratch_dir("contract-version");
        assert!(validate_contract_version_file(&root).is_err());

        fs::write(root.join("CONTRACT_VERSION"), "9.9.9\n").unwrap();
        let mismatch = validate_contract_version_file(&root).unwrap_err();
        assert!(mismatch.contains("9.9.9"));
        assert!(mismatch.contains(NIM_PARSER_CONTRACT_VERSION));

        fs::write(
            root.join("CONTRACT_VERSION"),
            format!("{NIM_PARSER_CONTRACT_VERSION}\n"),
        )
        .unwrap();
        validate_contract_version_file(&root).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn build_support_test_target_can_use_descriptor_and_sha_dependencies() {
        let descriptor = serde_json::json!({
            "contract_version": NIM_PARSER_CONTRACT_VERSION,
            "target": "x86_64-unknown-linux-gnu",
        });
        let encoded = serde_json::to_vec(&descriptor).unwrap();
        let digest = sha2::Sha256::digest(&encoded);

        assert_eq!(descriptor["contract_version"], NIM_PARSER_CONTRACT_VERSION);
        assert_eq!(digest.len(), 32);
    }

    #[test]
    fn inactive_descriptor_resolves_exact_legacy_target_identity() {
        let root = scratch_dir("descriptor-legacy");
        let target_dir = root.join("vendor/x86_64-unknown-linux-gnu");
        fs::create_dir_all(&target_dir).unwrap();
        let library = target_dir.join("libalopex_sql_parser.so");
        fs::write(&library, b"legacy-parser-bytes").unwrap();
        fs::write(target_dir.join("CONTRACT_VERSION"), "0.2.0\n").unwrap();
        let digest = hex_digest(&fs::read(&library).unwrap());
        fs::write(
            target_dir.join("SHA256SUMS"),
            format!("{digest}  libalopex_sql_parser.so\n"),
        )
        .unwrap();
        let descriptor_path = root.join("parser-consumer.json");
        fs::write(
            &descriptor_path,
            serde_json::to_vec(&serde_json::json!({
                "schema": "skulk-parser-consumer-v1",
                "active_mode": "legacy",
                "legacy": {
                    "source_repository": "alopex-db/alopex",
                    "source_ref": "v0.8.1",
                    "contract_version": "0.2.0",
                    "vendor_root": "vendor",
                    "contract_file": "CONTRACT_VERSION",
                    "sha256sums_file": "SHA256SUMS"
                },
                "public_release": {
                    "alopex_version": "0.8.4",
                    "contract_version": "0.4.0",
                    "vendor_root": "vendor/v0.8.4",
                    "contract_file": "CONTRACT_VERSION",
                    "sha256sums_file": "SHA256SUMS",
                    "manifest": "vendor/v0.8.4/parser-vendor-manifest-v0.8.4.json",
                    "envelope": "vendor/v0.8.4/parser-assets-v0.8.4.json",
                    "source": {"tag": "v0.8.4", "tag_sha": "9a0cea1d24e7672f59cae72d9218b9cc698d9162"}
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let descriptor = load_parser_consumer_descriptor(&descriptor_path).unwrap();
        let resolved =
            resolve_parser_target(&descriptor, "legacy", "x86_64-unknown-linux-gnu", &root)
                .unwrap();
        assert_eq!(resolved.source_ref, "v0.8.1");
        assert_eq!(resolved.contract_version, "0.2.0");
        assert_eq!(resolved.library_path, library);

        let override_root = root.join("override");
        fs::create_dir_all(&override_root).unwrap();
        let override_library = override_root.join("libalopex_sql_parser.so");
        fs::write(&override_library, b"override-parser-bytes").unwrap();
        fs::write(override_root.join("CONTRACT_VERSION"), "0.2.0\n").unwrap();
        let override_digest = hex_digest(&fs::read(&override_library).unwrap());
        fs::write(
            override_root.join("SHA256SUMS"),
            format!("{override_digest}  libalopex_sql_parser.so\n"),
        )
        .unwrap();
        let override_resolved = resolve_parser_target_with_override(
            &descriptor,
            "legacy",
            "x86_64-unknown-linux-gnu",
            &root,
            Some(&override_root),
        )
        .unwrap();
        assert_eq!(override_resolved.library_path, override_library);

        fs::write(
            override_root.join("SHA256SUMS"),
            "00  libalopex_sql_parser.so\n",
        )
        .unwrap();
        let error = resolve_parser_target_with_override(
            &descriptor,
            "legacy",
            "x86_64-unknown-linux-gnu",
            &root,
            Some(&override_root),
        )
        .unwrap_err();
        assert!(error.contains("SHA256SUMS"), "{error}");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tracked_parser_consumer_descriptor_is_valid_and_stays_inactive() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("nim-parser")
            .join("parser-consumer.json");
        let descriptor = load_parser_consumer_descriptor(&path).unwrap();
        assert_eq!(descriptor.value["active_mode"].as_str(), Some("legacy"));
        assert_eq!(
            descriptor.value["legacy"]["source_ref"].as_str(),
            Some("v0.8.1")
        );
    }

    #[test]
    fn staged_public_descriptor_resolves_every_target_offline() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("nim-parser");
        let descriptor =
            load_parser_consumer_descriptor(&root.join("parser-consumer.json")).unwrap();
        for target in VENDORED_TARGETS {
            let resolved = resolve_parser_target(&descriptor, "public_release", target, &root)
                .unwrap_or_else(|error| panic!("{target}: {error}"));
            assert_eq!(resolved.contract_version, "0.4.0");
            assert_eq!(resolved.target, target);
            assert!(resolved.manifest_path.is_some());
            assert!(resolved.envelope_path.is_some());
        }
    }

    #[test]
    fn inactive_descriptor_rejects_public_release_without_envelope() {
        let root = scratch_dir("descriptor-public-missing-envelope");
        let descriptor_path = root.join("parser-consumer.json");
        fs::write(
            &descriptor_path,
            serde_json::to_vec(&serde_json::json!({
                "schema": "skulk-parser-consumer-v1",
                "active_mode": "legacy",
                "legacy": {
                    "source_repository": "alopex-db/alopex",
                    "source_ref": "v0.8.1",
                    "contract_version": "0.2.0",
                    "vendor_root": "vendor",
                    "contract_file": "CONTRACT_VERSION",
                    "sha256sums_file": "SHA256SUMS"
                },
                "public_release": {
                    "alopex_version": "0.8.4",
                    "contract_version": "0.4.0",
                    "vendor_root": "vendor/v0.8.4",
                    "contract_file": "CONTRACT_VERSION",
                    "sha256sums_file": "SHA256SUMS",
                    "manifest": "vendor/v0.8.4/parser-vendor-manifest-v0.8.4.json",
                    "source": {"tag": "v0.8.4", "tag_sha": "9a0cea1d24e7672f59cae72d9218b9cc698d9162"}
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let error = load_parser_consumer_descriptor(&descriptor_path).unwrap_err();
        assert!(error.contains("public_release.envelope"), "{error}");
        fs::remove_dir_all(root).unwrap();
    }

    fn hex_digest(bytes: &[u8]) -> String {
        sha2::Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}
