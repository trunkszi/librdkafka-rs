#!/usr/bin/env bash
# Validate repository contracts and the affected packages with their reverse dependencies.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
base="${1:-}"
if ! git rev-parse --verify HEAD >/dev/null 2>&1; then
  base=""
elif [ -z "$base" ] || ! git cat-file -e "$base^{commit}" 2>/dev/null; then
  base="$(git rev-parse --verify HEAD~1 2>/dev/null || git rev-parse HEAD)"
fi
export CI_DIFF_BASE="$base"

python3 - <<'PY'
import pathlib
import re
import subprocess
import tomllib

files = subprocess.check_output(
    ['git', 'ls-files', '-co', '--exclude-standard', '-z']
).decode().split('\0')
forbidden = {'tokio', 'futures', 'futures-core', 'futures-util',
             'async-std', 'smol', 'openssl', 'openssl-sys'}
errors = []

def dependencies(value, path):
    for key, child in value.items():
        if key in {'dependencies', 'dev-dependencies', 'build-dependencies'}:
            for alias, declaration in child.items():
                if not isinstance(declaration, dict):
                    errors.append(f'{path}: {alias} must use an inline dependency table')
                    continue
                package = declaration.get('package', alias)
                if alias in forbidden or package in forbidden:
                    errors.append(f'{path}: forbidden dependency {package}')
        elif isinstance(child, dict):
            dependencies(child, path)

def canonical_keys(statement):
    value = tomllib.loads(statement + '\n__ci_probe__ = true')
    keys = []
    while True:
        if isinstance(value, list):
            value = value[-1]
        if '__ci_probe__' in value:
            break
        key, value = next(iter(value.items()))
        keys.append(key)
    return keys


def inline_dependencies(text, path):
    section = []
    multiline = None
    kinds = {'dependencies', 'dev-dependencies', 'build-dependencies'}
    for line in text.splitlines():
        # Ignore multiline string bodies; their text is not TOML declarations.
        quote = None
        assignment = None
        code = ''
        index = 0
        while index < len(line):
            if multiline:
                if line.startswith(multiline, index):
                    index += 3
                    multiline = None
                else:
                    index += 2 if multiline == '"""' and line[index] == '\\' else 1
                continue
            char = line[index]
            if quote:
                code += char
                if char == '\\' and quote == '"' and index + 1 < len(line):
                    code += line[index + 1]
                    index += 2
                    continue
                if char == quote:
                    quote = None
            elif char == '#':
                break
            elif line[index:index + 3] in ('"""', "'" * 3):
                multiline = line[index:index + 3]
                code += '""'
                index += 3
                continue
            else:
                if char in ('"', "'"):
                    quote = char
                elif char == '=' and assignment is None:
                    assignment = len(code)
                code += char
            index += 1
        code = code.strip()
        if not code:
            continue
        if code.startswith('['):
            section = canonical_keys(code)
            if any(key in kinds for key in section[:-1]):
                errors.append(f'{path}: dependency subtables must use inline-table syntax')
        elif assignment is not None:
            # Locate the delimiter again after leading whitespace was stripped.
            lhs = line[:assignment].strip()
            rhs = line[assignment + 1:].lstrip()
            keys = canonical_keys('[' + lhs + ']')
            location = section + keys
            for position, key in enumerate(location):
                if key in kinds and (len(location) != position + 2 or not rhs.startswith('{')):
                    errors.append(f'{path}: dependencies must be direct inline-table declarations')


for name in sorted(set(files)):
    path = pathlib.Path(name)
    if not path.is_file():
        continue
    if path.suffix in {'.rs', '.cpp', '.h', '.hpp'}:
        text = path.read_text()
        count = len(text.splitlines())
        if count > 800:
            errors.append(f'{path}: {count} lines exceeds 800')
        if name.startswith(('crates/rdkafka/', 'crates/rdkafka-macros/')):
            if re.search(r'\b(?:Cpp\w*|cpp_\w*)\b', text):
                errors.append(f'{path}: forbidden Cpp*/cpp_* identifier')
    if name.endswith('Cargo.toml'):
        text = path.read_text()
        dependencies(tomllib.loads(text), path)
        inline_dependencies(text, path)
    if path.name == 'Cargo.lock':
        for package in tomllib.loads(path.read_text()).get('package', []):
            if package['name'] in forbidden:
                errors.append(f'{path}: forbidden resolved dependency {package["name"]}')
if errors:
    raise SystemExit('\n'.join(errors))
print('Source size, manifest syntax, dependency and identifier contracts passed.')
PY

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
