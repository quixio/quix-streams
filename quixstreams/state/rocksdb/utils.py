import re
from pathlib import Path

ROCKSDB_CORRUPTION_REGEX = re.compile(
    r"^Corruption: Corruption: IO error: No such file or "
    r"directory: While open a file for random read: .*: "
    r"No such file or directory  The file .* may be corrupted\.$"
)
ROCKSDB_FILES_REGEX = re.compile(
    r"^("
    r"(rocksdict-config.json|CURRENT|IDENTITY|LOCK)"
    r"|"
    r"LOG(\.old\.\d+)?"
    r"|"
    r"(MANIFEST|OPTIONS)-\d+"
    r"|"
    r"\d+\.(log|sst)"
    r")$"
)


def is_rocksdb_corruption_error(error: Exception) -> bool:
    """
    Check if the error is a RocksDB corruption error.
    """
    return ROCKSDB_CORRUPTION_REGEX.fullmatch(str(error)) is not None


def is_rocksdb_directory(path: str) -> bool:
    """
    Verify if a directory is a RocksDB database by checking that
    all files are RocksDB-specific files. This works even
    if the database is corrupted.
    """
    _path = Path(path)
    if not _path.is_dir():
        return False

    files = [f.name for f in _path.iterdir()]
    if not files:  # Empty directory is not a valid RocksDB
        return False

    # Check that all files are RocksDB-specific files
    for file in files:
        if not ROCKSDB_FILES_REGEX.fullmatch(file):
            return False

    return True
