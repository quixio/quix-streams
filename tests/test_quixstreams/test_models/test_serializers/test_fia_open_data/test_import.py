"""Import-time behaviour of the FIA Open Data generated code."""

import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]


def _version(text: str) -> tuple[int, ...]:
    return tuple(map(int, text.split(".")))


def test_import_quixstreams_does_not_load_open_data_pb2():
    """Importing quixstreams does not import open_data_pb2; previously the serializers package imported it eagerly."""
    code = "import sys, quixstreams; sys.exit(any('open_data_pb2' in m for m in sys.modules))"
    result = subprocess.run([sys.executable, "-c", code], check=False)  # noqa: S603
    assert result.returncode == 0


def test_checked_in_gencode_not_newer_than_protobuf_floor():
    """No checked-in *_pb2.py targets a protobuf version above the protobuf extra's floor; previously open_data_pb2.py targeted 6.31.1."""
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    requirement = pyproject["project"]["optional-dependencies"]["protobuf"][0]
    floor = _version(re.search(r">=(\d+\.\d+\.\d+)", requirement).group(1))
    gencode = [
        *ROOT.glob("quixstreams/**/*_pb2.py"),
        *ROOT.glob("tests/**/*_pb2.py"),
    ]
    for path in gencode:
        marker = re.search(
            r"# Protobuf Python Version: (\d+\.\d+\.\d+)",
            path.read_text(encoding="utf-8"),
        )
        assert _version(marker.group(1)) <= floor, path
