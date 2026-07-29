//! Resolves the vendored Nim query-parser shared library for the text query
//! frontends (`promql` / `sql-ts` features). The core profile emits nothing.
//!
//! Resolution order:
//! 1. `SKULK_NIM_PARSER_LIB_DIR` (development / CI override)
//! 2. `crates/skulk/nim-parser/vendor/<target-triple>/`
//!
//! Missing artifacts are tolerated until the Nim parser lands (spec task 3.1):
//! nothing links against the library before the FFI bridge exists. Once it
//! does, the final executable's build script receives the directory via
//! `DEP_SKULK_NIM_PARSER_LIBDIR` and is responsible for setting an rpath
//! (same convention as alopex-cli with alopex-sql).

mod build_support;

use build_support::{nim_lib_filename_for, resolve_library_dir};
use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=SKULK_NIM_PARSER_LIB_DIR");
    let promql = env::var_os("CARGO_FEATURE_PROMQL").is_some();
    let sql_ts = env::var_os("CARGO_FEATURE_SQL_TS").is_some();
    if !(promql || sql_ts) {
        return;
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let target = env::var("TARGET").expect("TARGET");
    let target_os = env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS");
    let lib_filename = nim_lib_filename_for(&target_os)
        .unwrap_or_else(|error| panic!("{error} (target={target})"));
    let override_dir = env::var_os("SKULK_NIM_PARSER_LIB_DIR").map(PathBuf::from);
    let vendored_dir = manifest_dir.join("nim-parser/vendor").join(&target);
    let lib_path = override_dir
        .as_deref()
        .unwrap_or(&vendored_dir)
        .join(lib_filename);
    println!("cargo:rerun-if-changed={}", lib_path.display());
    println!(
        "cargo:rerun-if-changed={}",
        vendored_dir.join("CONTRACT_VERSION").display()
    );

    let lib_dir = resolve_library_dir(
        &manifest_dir,
        &target,
        lib_filename,
        override_dir.as_deref(),
    )
    .unwrap_or_else(|error| panic!("{error} (target={target})"));
    let Some(lib_dir) = lib_dir else {
        return;
    };

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    if target_os != "windows" {
        println!("cargo:rustc-link-lib=dylib=alopex_sql_parser");
    }
    println!("cargo::metadata=libdir={}", lib_dir.display());

    if target_os == "linux" || target_os == "macos" {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
        println!(
            "cargo:rustc-link-arg-tests=-Wl,-rpath,{}",
            lib_dir.display()
        );
    }
}
