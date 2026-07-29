use std::path::{Path, PathBuf};

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

pub(crate) fn is_vendored_target(target: &str) -> bool {
    VENDORED_TARGETS.contains(&target)
}

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

#[cfg(test)]
mod tests {
    use super::*;
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
}
