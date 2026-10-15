import importlib
import sys

import pytest

from quixstreams.models.serializers import fia_open_data
from quixstreams.models.serializers.fia_open_data import (
    FIAOpenDataDeserializer,
    open_data_pb2,
)

OLD_PATH = "quixstreams.models.serializers.ma_streaming_open_data"
NEW_PATH = "quixstreams.models.serializers.fia_open_data"


def _import_old_path():
    sys.modules.pop(OLD_PATH, None)
    with pytest.warns(DeprecationWarning) as record:
        module = importlib.import_module(OLD_PATH)
    return module, record


def test_old_path_warns_once_naming_new_path():
    _, record = _import_old_path()
    deprecations = [w for w in record if issubclass(w.category, DeprecationWarning)]
    assert len(deprecations) == 1
    assert NEW_PATH in str(deprecations[0].message)


def test_old_class_name_is_new_class():
    old, _ = _import_old_path()
    assert old.MAStreamingDeserializer is FIAOpenDataDeserializer


def test_old_protocol_version_matches():
    old, _ = _import_old_path()
    assert old.__protocol_version__ == fia_open_data.__protocol_version__


def test_old_open_data_pb2_is_same_module():
    old, _ = _import_old_path()
    assert old.open_data_pb2 is open_data_pb2
