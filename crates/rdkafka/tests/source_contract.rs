//! Native and Rust source boundaries for removed HTTP backends.
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn owned_source(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("rs" | "c" | "cpp" | "cc" | "h" | "hpp")
    )
}

fn removed_backend(source: &str) -> bool {
    let backend = ["cu", "rl"].concat();
    let native_prefix = format!("{backend}_");
    let uppercase = backend.to_ascii_uppercase();
    let memory_file = ["mem", "fd"].concat();
    let json = ["serde", "_json"].concat();
    source
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .any(|token| {
            token == backend
                || token.starts_with(&native_prefix)
                || token.starts_with(&uppercase)
                || token == memory_file
                || token == json
        })
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn verify_owned_sources(directory: &Path) -> Result<(), String> {
    let output = Command::new("git")
        .args(["ls-files", "-co", "--exclude-standard", "-z"])
        .current_dir(directory)
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err("cannot enumerate owned source files".into());
    }
    for name in String::from_utf8(output.stdout)
        .map_err(|error| error.to_string())?
        .split('\0')
    {
        let path = directory.join(name);
        if owned_source(&path)
            && path.is_file()
            && removed_backend(&fs::read_to_string(&path).map_err(|error| error.to_string())?)
        {
            return Err(format!("{name}: forbidden HTTP or JSON backend reference"));
        }
    }
    Ok(())
}

#[test]
fn integration_scenario_owned_rust_and_native_sources_exclude_removed_backends() {
    verify_owned_sources(&root()).expect("owned source contract");
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rdkafka-source-contract-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        assert!(
            Command::new("git")
                .args(["init", "-q"])
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            eprintln!("source fixture cleanup failed: {error}");
            assert!(std::thread::panicking(), "source fixture was not removed");
        }
    }
}

#[test]
fn integration_scenario_governance_rejects_native_http_includes_and_calls() {
    let backend = ["cu", "rl"].concat();
    let sources = [
        format!("#include <{backend}/{backend}.h>\n"),
        format!("void *backend() {{ return {backend}_easy_init(); }}\n"),
        format!("{} *handle;\n", backend.to_ascii_uppercase()),
    ];
    for extension in ["c", "cpp", "cc", "h", "hpp"] {
        for source in &sources {
            let fixture = Fixture::new();
            let path = fixture
                .0
                .join(format!("crates/demo/src/backend.{extension}"));
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, source).unwrap();
            assert!(removed_backend(source));
            let result = verify_owned_sources(&fixture.0);
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .contains("forbidden HTTP or JSON backend reference")
            );
        }
    }
    assert!(!removed_backend("const char *option = \"WITH_CURL=OFF\";"));
}
