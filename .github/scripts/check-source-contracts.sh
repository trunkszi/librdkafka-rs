#!/usr/bin/env bash
# Validate source size, manifest syntax, and dependency boundaries.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
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
