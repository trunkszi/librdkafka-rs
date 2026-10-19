"""Regression cases for manifest syntax in the repository CI boundary."""
import pathlib
import subprocess
import tempfile
import unittest

SCRIPT = str(pathlib.Path(__file__).with_name('ci-incremental.sh'))

class ManifestSyntax(unittest.TestCase):
    def gate(self, manifest, accepted=False):
        with tempfile.TemporaryDirectory(prefix='rdkafka-inline-') as d:
            subprocess.run(['git', 'init', '-q', d], check=True)
            path = pathlib.Path(d) / 'crates/demo/Cargo.toml'
            path.parent.mkdir(parents=True)
            path.write_text(manifest)
            result = subprocess.run(['bash', SCRIPT], cwd=d, capture_output=True, text=True)
            self.assertEqual(result.returncode == 0, accepted, result.stdout + result.stderr)
            if not accepted:
                self.assertIn('inline', result.stderr)
    def test_dotted_declaration(self):
        self.gate('[dependencies]\nserde.version="1"\n')
    def test_quoted_subtable(self):
        self.gate('["dependencies"."serde"]\nversion="1"\n')
    def test_workspace_quoted_subtable(self):
        self.gate('[workspace."dependencies".serde]\nversion="1"\n')
    def test_valid_quoted_inline_and_noncode(self):
        self.gate('[package]\nname="demo"\ndescription="""\n[dependencies.fake]\nfoo.version="1"\n"""\n[[bin]]\nname="demo"\n[target.\'cfg(unix)\'."dependencies"]\n"serde" = { version="1" }\n', True)

if __name__ == '__main__':
    unittest.main()
