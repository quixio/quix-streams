"""Regenerate ``open_data_pb2.py`` from ``proto/open_data.proto``.

Developer-only helper. Not invoked at install time. Run after bumping the
bundled schema to a new MA Streaming Open Data revision.

Requirements
------------

- ``grpcio-tools >= 1.62`` (ships ``grpc_tools.protoc`` with a bundled
  protoc compiler; no system-level ``protoc`` binary needed).
- The bundled compiler in ``grpcio-tools`` 1.62+ is protoc 26+, which
  matches the ``# Protobuf Python Version: 6.31.1`` marker at the top of
  the committed ``open_data_pb2.py``. If you produce a file with a
  noticeably different marker, double-check your ``grpcio-tools`` version
  before committing.

Usage
-----

From the repo root::

    python -m pip install grpcio-tools
    python -m quixstreams.models.serializers.ma_streaming_open_data.regenerate

The generated ``open_data_pb2.py`` is committed alongside this script so
end-users (``pip install quixstreams``) do not need a protoc toolchain.
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROTO_SRC_DIR = HERE / "proto"
PROTO_OUT_DIR = HERE
PROTO_FILE = "open_data.proto"


def main() -> int:
    try:
        from grpc_tools import protoc  # type: ignore[import-not-found]
    except ImportError:
        print(
            "grpcio-tools is required to regenerate protobuf bindings.\n"
            "Install it with: pip install grpcio-tools",
            file=sys.stderr,
        )
        return 1

    # grpc_tools ships the google/* well-known proto files alongside the
    # package; include that directory so imports like
    # ``google/protobuf/timestamp.proto`` resolve without a system protoc.
    import grpc_tools

    well_known = Path(grpc_tools.__file__).parent / "_proto"

    args = [
        "protoc",
        f"--proto_path={PROTO_SRC_DIR}",
        f"--proto_path={well_known}",
        f"--python_out={PROTO_OUT_DIR}",
        str(PROTO_SRC_DIR / PROTO_FILE),
    ]
    print("Running:", " ".join(args))
    rc = protoc.main(args)
    if rc != 0:
        print(f"protoc failed with exit code {rc}", file=sys.stderr)
        return rc

    out_file = PROTO_OUT_DIR / "open_data_pb2.py"
    if not out_file.exists():
        print(f"Expected output not found: {out_file}", file=sys.stderr)
        return 2
    print(f"Generated: {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
