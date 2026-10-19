#!/usr/bin/env bash
# Validate repository contracts and the affected packages with their reverse dependencies.
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$(git rev-parse --show-toplevel)"
base="${1:-}"
if ! git rev-parse --verify HEAD >/dev/null 2>&1; then
  base=""
elif [ -z "$base" ] || ! git cat-file -e "$base^{commit}" 2>/dev/null; then
  base="$(git rev-parse --verify HEAD~1 2>/dev/null || git rev-parse HEAD)"
fi
export CI_DIFF_BASE="$base"

bash "$script_dir/check-source-contracts.sh"

if [ ! -f Cargo.toml ]; then
  echo 'No root manifest yet; governance bootstrap checks completed.'
  exit 0
fi

# Resolve transitive reverse dependencies against the actual workspace package graph.
selected="$(python3 - <<'PY'
import json
import os
import pathlib
import subprocess

metadata = json.loads(subprocess.check_output(
    ['cargo', 'metadata', '--format-version', '1'], text=True))
forbidden = {'tokio', 'futures', 'futures-core', 'futures-util',
             'async-std', 'smol', 'openssl', 'openssl-sys'}
resolved_forbidden = sorted({p['name'] for p in metadata['packages']} & forbidden)
if resolved_forbidden:
    raise SystemExit('Forbidden resolved dependencies: ' + ', '.join(resolved_forbidden))
root = pathlib.Path.cwd()
packages = {p['name']: p for p in metadata['packages']
            if p['id'] in metadata['workspace_members']}
base = os.environ['CI_DIFF_BASE']
if base and base != subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip():
    changed = subprocess.check_output(['git', 'diff', '--name-only', '-z', base + '...HEAD'])
else:
    changed = subprocess.check_output(['git', 'ls-files', '-z'])
changed += subprocess.check_output(['git', 'diff', '--name-only', '-z', 'HEAD'])
changed += subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z'])
files = [x for x in changed.decode().split('\0') if x]
all_packages = any(p in {'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'rustfmt.toml'}
                   or p.startswith(('.github/', '.cargo/')) for p in files)
selected = set(packages) if all_packages else set()
for name, package in packages.items():
    directory = pathlib.Path(package['manifest_path']).parent.relative_to(root).as_posix() + '/'
    if any(p.startswith(directory) for p in files):
        selected.add(name)
while True:
    previous = set(selected)
    for name, package in packages.items():
        if any(d['name'] in selected for d in package['dependencies']):
            selected.add(name)
    if previous == selected:
        break
priority = {'rdkafka-sys': 0, 'rdkafka-macros': 1, 'rdkafka': 2}
print('\n'.join(sorted(selected, key=lambda n: (priority.get(n, 3), n))))
PY
)"
if [ -z "$selected" ]; then
  echo 'Only governance or documentation changed; no affected Cargo packages.'
  exit 0
fi
cargo fmt --all -- --check
while IFS= read -r package; do
  cargo clippy -p "$package" --all-targets -- -D warnings
  if [ "$package" = rdkafka ]; then
    cargo clippy -p "$package" --all-targets --features tracing -- -D warnings
    cargo clippy -p "$package" --all-targets --features tls -- -D warnings
    cargo clippy -p "$package" --all-targets --features http,tracing -- -D warnings
  fi
  if command -v cargo-nextest >/dev/null 2>&1; then
    cargo nextest run -p "$package" --no-tests=pass
    cargo test -p "$package" --doc
    if [ "$package" = rdkafka ]; then
      cargo nextest run -p "$package" --features tls --no-tests=pass
      cargo nextest run -p "$package" --features http --no-tests=pass
      cargo test -p "$package" --features http --doc
    fi
  else
    cargo test -p "$package"
    if [ "$package" = rdkafka ]; then
      cargo test -p "$package" --features tls
      cargo test -p "$package" --features http
    fi
  fi
  if [ "$package" = rdkafka ]; then
    bash .github/scripts/test-redpanda.sh
    bash .github/scripts/test-broker-security.sh
  fi
done <<< "$selected"
