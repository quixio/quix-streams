"""
Red-first coverage for unconditional stamp-stripping in the adoption rollback.

``_rollback_provisional_adopt`` decides per default-CF key:

  * value == its backup entry  -> untouched adopted original, leave verbatim
  * otherwise                  -> post-adoption write, strip 8 bytes

The second branch is unconditional (``partition.py``)::

    reverted_puts.append((raw_key, current_value[TTL_STAMP_BYTES:]))

There is no length check and no ``_safe_decode_stamp`` validation, so it does not
verify the value actually carries a stamp before removing eight bytes. Any
default-CF value that differs from its backup entry -- or is absent from the
backup -- is truncated: a 7-byte value becomes ``b''``.

Why this matters more than an ordinary edge case: rollback is the ONLY mitigation
for the accepted false-positive risk, where a legacy 8-byte store is
indistinguishable from a v3.24.0 store on cold restore and gets adopted. If the
escape hatch itself destroys short values, that accepted risk has no remedy.
"""

from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.metadata import METADATA_CF_NAME, TTL_ADOPT_BACKUP_CF_NAME
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.metadata import TTL_ADOPT_PENDING_KEY
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000
NOW_MS = 1_780_000_000_000


def _make_producer():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _v3240_msg(key_str, user_value, expiry_ms, prefix=b"pfx"):
    """One v3.24.0-style stamped default-CF changelog message."""
    raw_key = prefix + b"|" + json_dumps(key_str)
    return raw_key, encode_ttl_value(expiry_ms, json_dumps(user_value)), False


def _provision_partition(tmp_path, name="db", n_keys=2):
    """Cold-provisionally-adopted partition: backup CF present, pending marker set."""
    producer = _make_producer()
    partition = RocksDBStorePartition(
        (tmp_path / name).as_posix(),
        options=RocksDBOptions(open_max_retries=0),
        changelog_producer=producer,
    )
    partition._now_ms = lambda: NOW_MS  # noqa: E731
    expiry = NOW_MS + 7 * DAY_MS
    for offset, (key, value, stamped) in enumerate(
        _v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(n_keys)
    ):
        partition.recover_from_changelog_message(
            key=key, value=value, cf_name="default", offset=offset, ttl_stamped=stamped
        )
    partition.complete_recovery()

    assert partition._adopt_provisional is True, "precondition: provisional adopt"
    metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
    assert metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is not None
    return partition


class TestRollbackDoesNotTruncateShortValues:
    def test_rollback_preserves_a_short_unstamped_value(self, tmp_path, monkeypatch):
        """
        A default-CF value that is NOT in the backup and is shorter than the
        8-byte stamp must survive the rollback intact, not be truncated to
        nothing.

        Such a value is exactly what a legacy store holds -- ``json_dumps(1)`` is
        ``b'1'`` -- and a legacy store falsely adopted on cold restore is the case
        rollback exists to undo.
        """
        partition = _provision_partition(tmp_path)
        try:
            default_cf = partition.get_or_create_column_family("default")
            default_handle = partition.get_column_family_handle("default")
            backup_cf = partition.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)

            # A short legacy value present in the default CF but absent from the
            # backup snapshot -> takes the "post-adoption write" branch.
            short_key = b"pfx|" + json_dumps("short")
            short_value = json_dumps(1)  # b'1' -- one byte
            assert len(short_value) < 8
            assert backup_cf.get(short_key, default=None) is None

            from rocksdict import WriteBatch

            batch = WriteBatch(raw_mode=True)
            batch.put(short_key, short_value, default_handle)
            partition._write(batch)
            assert bytes(default_cf[short_key]) == short_value

            monkeypatch.setenv("QUIXSTREAMS_STATE_TTL_ROLLBACK", "1")
            partition._rollback_provisional_adopt()

            survived = default_cf.get(short_key, default=None)
            assert survived is not None, "the short value was deleted by rollback"
            assert bytes(survived) == short_value, (
                f"rollback truncated a {len(short_value)}-byte value to "
                f"{bytes(survived)!r}: it stripped 8 stamp bytes from a value that "
                f"carries no stamp"
            )
        finally:
            partition.close()
