from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from quixstreams.sources.community.file.s3 import S3FileSource


@pytest.fixture
def source():
    source = S3FileSource(filepath="data", bucket="test-bucket")
    source._client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    yield source
    source._close_client()


def test_get_file_list_reads_all_pages(source):
    keys = [f"data/{index:04}.json" for index in range(1001)]
    params = {"Bucket": "test-bucket", "Prefix": "data", "Delimiter": "/"}
    with Stubber(source._client) as stubber:
        stubber.add_response(
            "list_objects",
            {
                "IsTruncated": True,
                "Contents": [{"Key": key, "Size": 1} for key in keys[:1000]],
            },
            params,
        )
        stubber.add_response(
            "list_objects",
            {"IsTruncated": False, "Contents": [{"Key": keys[-1], "Size": 1}]},
            {**params, "Marker": keys[999]},
        )

        assert list(source.get_file_list(Path("data"))) == [Path(key) for key in keys]
        stubber.assert_no_pending_responses()


def test_get_file_list_recurses_into_later_pages_and_skips_folder_markers(source):
    params = {"Bucket": "test-bucket", "Prefix": "data", "Delimiter": "/"}
    with Stubber(source._client) as stubber:
        stubber.add_response(
            "list_objects",
            {
                "IsTruncated": True,
                "NextMarker": "data/a.json",
                "Contents": [{"Key": "data/a.json", "Size": 1}],
            },
            params,
        )
        stubber.add_response(
            "list_objects",
            {
                "IsTruncated": False,
                "CommonPrefixes": [{"Prefix": "data/nested/"}],
            },
            {**params, "Marker": "data/a.json"},
        )
        stubber.add_response(
            "list_objects",
            {
                "IsTruncated": False,
                "Contents": [
                    {"Key": "data/nested/", "Size": 0},
                    {"Key": "data/nested/b.json", "Size": 1},
                ],
            },
            {**params, "Prefix": "data/nested/"},
        )

        assert list(source.get_file_list(Path("data"))) == [
            Path("data/a.json"),
            Path("data/nested/b.json"),
        ]
        stubber.assert_no_pending_responses()


def test_file_partition_counter_reads_all_pages(source):
    params = {"Bucket": "test-bucket", "Prefix": "data/", "Delimiter": "/"}
    with Stubber(source._client) as stubber:
        stubber.add_response(
            "list_objects",
            {
                "IsTruncated": True,
                "NextMarker": "data/0/",
                "CommonPrefixes": [{"Prefix": "data/0/"}],
            },
            params,
        )
        stubber.add_response(
            "list_objects",
            {"IsTruncated": False, "CommonPrefixes": [{"Prefix": "data/1/"}]},
            {**params, "Marker": "data/0/"},
        )
        assert source.file_partition_counter() == 2
        assert source._client is None
        stubber.assert_no_pending_responses()


def test_get_file_list_empty_prefix(source):
    with Stubber(source._client) as stubber:
        stubber.add_response(
            "list_objects",
            {"IsTruncated": False},
            {"Bucket": "test-bucket", "Prefix": "data", "Delimiter": "/"},
        )
        assert list(source.get_file_list(Path("data"))) == []
        stubber.assert_no_pending_responses()


def test_file_partition_counter_closes_client_on_listing_error(source):
    with Stubber(source._client) as stubber:
        stubber.add_response(
            "list_objects",
            {
                "IsTruncated": True,
                "NextMarker": "data/0/",
                "CommonPrefixes": [{"Prefix": "data/0/"}],
            },
            {"Bucket": "test-bucket", "Prefix": "data/", "Delimiter": "/"},
        )
        stubber.add_client_error("list_objects", service_error_code="AccessDenied")
        with pytest.raises(ClientError, match="AccessDenied"):
            source.file_partition_counter()
        assert source._client is None
        stubber.assert_no_pending_responses()


def test_file_partition_counter_empty_prefix(source):
    with Stubber(source._client) as stubber:
        stubber.add_response(
            "list_objects",
            {"IsTruncated": False},
            {"Bucket": "test-bucket", "Prefix": "data/", "Delimiter": "/"},
        )
        assert source.file_partition_counter() == 0
        assert source._client is None
        stubber.assert_no_pending_responses()
