//! Portable, statically linked Kafka native core and compression codec build.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const KAFKA_REV: &str = "c6c9973cf1b25394c58e0cade13a36f386856f77";
const ZSTD_REV: &str = "f8745da6ff1ad1e7bab384bd1f9d742439278e99";
const ZLIB_REV: &str = "da607da739fa6047df13e66a2af6b8bec7c2a498";

fn run(command: &mut Command) {
    let status = command
        .status()
        .expect("execute native source provisioning");
    assert!(
        status.success(),
        "native source command failed: {command:?}"
    );
}

fn revision(path: &Path) -> String {
    let result = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("read native source revision");
    assert!(
        result.status.success(),
        "cannot verify native source revision"
    );
    String::from_utf8(result.stdout)
        .expect("Git revision is UTF-8")
        .trim()
        .to_owned()
}

fn cached_source(cache: &Path, name: &str, repo: &str, tag: &str, rev: &str) -> PathBuf {
    fs::create_dir_all(cache).expect("create native source cache");
    let source = cache.join(name);
    if !source.is_dir() {
        // Per-process staging prevents incomplete clones from becoming cache hits.
        let staging = cache.join(format!("{name}.fetch-{}", std::process::id()));
        run(Command::new("git")
            .args(["clone", "--depth", "1", "--branch", tag, repo])
            .arg(&staging));
        assert_eq!(revision(&staging), rev, "native source tag moved");
        if let Err(error) = fs::rename(&staging, &source) {
            assert!(source.is_dir(), "publish native source: {error}");
            fs::remove_dir_all(staging).expect("remove duplicate verified clone");
        }
    }
    assert_eq!(revision(&source), rev, "unexpected native cache revision");
    source
}

fn cache_dir(manifest: &Path, out: &Path) -> PathBuf {
    let target = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                manifest.join("../..").join(path)
            }
        })
        .unwrap_or_else(|| {
            out.ancestors()
                .find(|path| path.file_name().is_some_and(|name| name == "build"))
                .and_then(Path::parent)
                .and_then(Path::parent)
                .expect("Cargo target directory")
                .to_path_buf()
        });
    target.join("vendor-cache")
}

fn kafka_source(manifest: &Path, cache: &Path) -> PathBuf {
    if let Some(configured) = env::var_os("LIBRDKAFKA_DIR") {
        let source = PathBuf::from(configured);
        assert!(
            source.join("src-cpp/rdkafkacpp.h").is_file(),
            "invalid LIBRDKAFKA_DIR"
        );
        return source;
    }
    let vendor = manifest.join("../../vendor/librdkafka");
    if vendor.join("src-cpp/rdkafkacpp.h").is_file() {
        return vendor;
    }
    cached_source(
        cache,
        "librdkafka",
        "https://github.com/confluentinc/librdkafka.git",
        "v2.16.0",
        KAFKA_REV,
    )
}

fn config(source: &Path, out: &Path) -> cmake::Config {
    let mut cfg = cmake::Config::new(source);
    cfg.out_dir(out)
        .profile("Release")
        .define("CMAKE_INSTALL_LIBDIR", "lib")
        .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON");
    cfg
}

fn archive(prefix: &Path, names: &[&str]) -> PathBuf {
    for name in names {
        for file in [format!("lib{name}.a"), format!("{name}.lib")] {
            let path = prefix.join("lib").join(file);
            if path.is_file() {
                return path;
            }
        }
    }
    panic!(
        "missing static native archive {names:?} in {}",
        prefix.display()
    );
}

fn link_archive(path: &Path) {
    let stem = path
        .file_stem()
        .expect("archive filename")
        .to_string_lossy();
    let name = if path.extension().is_some_and(|ext| ext == "a") {
        stem.strip_prefix("lib").expect("Unix archive prefix")
    } else {
        &stem
    };
    println!("cargo:rustc-link-lib=static={name}");
}

fn main() {
    println!("cargo:rerun-if-env-changed=LIBRDKAFKA_DIR");
    for file in [
        "src/lib.rs",
        "src/shim.h",
        "src/shim.cpp",
        "src/client.cpp",
        "src/consumer.cpp",
    ] {
        println!("cargo:rerun-if-changed={file}");
    }
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("build output directory"));
    let cache = cache_dir(&manifest, &out);
    let kafka = kafka_source(&manifest, &cache);
    let zlib = cached_source(
        &cache,
        "zlib",
        "https://github.com/madler/zlib.git",
        "v1.3.2",
        ZLIB_REV,
    );
    let zstd = cached_source(
        &cache,
        "zstd",
        "https://github.com/facebook/zstd.git",
        "v1.5.7",
        ZSTD_REV,
    );
    let zlib_dst = config(&zlib, &out.join("zlib"))
        .define("ZLIB_BUILD_SHARED", "OFF")
        .define("ZLIB_BUILD_STATIC", "ON")
        .define("ZLIB_BUILD_TESTING", "OFF")
        .build();
    let zstd_dst = config(&zstd.join("build/cmake"), &out.join("zstd"))
        .define("ZSTD_BUILD_SHARED", "OFF")
        .define("ZSTD_BUILD_STATIC", "ON")
        .define("ZSTD_BUILD_PROGRAMS", "OFF")
        .define("ZSTD_BUILD_TESTS", "OFF")
        .build();
    let zlib_archive = archive(&zlib_dst, &["z", "zs"]);
    let zstd_archive = archive(&zstd_dst, &["zstd", "zstd_static"]);
    let dst = config(&kafka, &out.join("kafka"))
        .define("RDKAFKA_BUILD_STATIC", "ON")
        .define("RDKAFKA_BUILD_EXAMPLES", "OFF")
        .define("RDKAFKA_BUILD_TESTS", "OFF")
        .define("WITH_SSL", "OFF")
        .define("WITH_CURL", "OFF")
        .define("WITH_PLUGINS", "OFF")
        .define("WITH_SASL", "OFF")
        .define("ENABLE_LZ4_EXT", "OFF")
        .define("WITH_ZLIB", "ON")
        .define("WITH_ZSTD", "ON")
        .define("ZLIB_LIBRARY", &zlib_archive)
        .define("ZLIB_INCLUDE_DIR", zlib_dst.join("include"))
        .define("ZSTD_LIBRARY", &zstd_archive)
        .define("ZSTD_LIBRARY_RELEASE", &zstd_archive)
        .define("ZSTD_INCLUDE_DIR", zstd_dst.join("include"))
        .build();
    cxx_build::bridge("src/lib.rs")
        .files(["src/shim.cpp", "src/client.cpp", "src/consumer.cpp"])
        .include(&manifest)
        .include(dst.join("include/librdkafka"))
        .define("LIBRDKAFKA_STATICLIB", None)
        .std("c++17")
        .compile("rdkafka-bridge");
    for prefix in [&dst, &zlib_dst, &zstd_dst] {
        println!(
            "cargo:rustc-link-search=native={}",
            prefix.join("lib").display()
        );
    }
    link_archive(&archive(&dst, &["rdkafka++"]));
    link_archive(&archive(&dst, &["rdkafka"]));
    link_archive(&zstd_archive);
    link_archive(&zlib_archive);
    let target = env::var("CARGO_CFG_TARGET_OS").expect("target operating system");
    if target == "windows" {
        for lib in ["ws2_32", "secur32", "crypt32"] {
            println!("cargo:rustc-link-lib={lib}");
        }
    } else {
        println!("cargo:rustc-link-lib=pthread");
        if target == "linux" {
            println!("cargo:rustc-link-lib=dl");
            println!("cargo:rustc-link-lib=rt");
        }
    }
}
