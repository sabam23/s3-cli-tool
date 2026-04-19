from datetime import datetime, timezone

import pytest

from s3_scli_tool.s3_service import (
    get_bucket_versioning_status,
    list_object_versions_info,
    restore_previous_object_version,
)


def test_get_bucket_versioning_status_enabled() -> None:
    class FakeClient:
        def get_bucket_versioning(self, **kwargs):
            return {"Status": "Enabled", "MFADelete": "Disabled"}

    status = get_bucket_versioning_status(FakeClient(), "demo-bucket")

    assert status["bucket_name"] == "demo-bucket"
    assert status["status"] == "Enabled"
    assert status["versioning_enabled"] is True
    assert status["mfa_delete"] == "Disabled"


def test_get_bucket_versioning_status_disabled_when_missing() -> None:
    class FakeClient:
        def get_bucket_versioning(self, **kwargs):
            return {}

    status = get_bucket_versioning_status(FakeClient(), "demo-bucket")

    assert status["status"] == "Disabled"
    assert status["versioning_enabled"] is False


def test_list_object_versions_info() -> None:
    latest = datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)
    previous = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)

    class FakePaginator:
        def paginate(self, **kwargs):
            return [
                {
                    "Versions": [
                        {
                            "Key": "uploads/photo.png",
                            "VersionId": "v1",
                            "IsLatest": False,
                            "LastModified": previous,
                        },
                        {
                            "Key": "uploads/photo.png",
                            "VersionId": "v2",
                            "IsLatest": True,
                            "LastModified": latest,
                        },
                        {
                            "Key": "uploads/other.png",
                            "VersionId": "skip",
                            "IsLatest": True,
                            "LastModified": latest,
                        },
                    ]
                }
            ]

    class FakeClient:
        def get_paginator(self, name: str):
            assert name == "list_object_versions"
            return FakePaginator()

    versions = list_object_versions_info(FakeClient(), "demo-bucket", "uploads/photo.png")

    assert versions["version_count"] == 2
    assert versions["versions"][0]["version_id"] == "v2"
    assert versions["versions"][0]["last_modified"] == latest.isoformat()
    assert versions["versions"][1]["version_id"] == "v1"


def test_restore_previous_object_version() -> None:
    latest = datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)
    previous = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)

    class FakePaginator:
        def paginate(self, **kwargs):
            return [
                {
                    "Versions": [
                        {
                            "Key": "uploads/photo.png",
                            "VersionId": "older-version",
                            "IsLatest": False,
                            "LastModified": previous,
                        },
                        {
                            "Key": "uploads/photo.png",
                            "VersionId": "latest-version",
                            "IsLatest": True,
                            "LastModified": latest,
                        },
                    ]
                }
            ]

    class FakeClient:
        def __init__(self) -> None:
            self.copy_calls = []

        def get_paginator(self, name: str):
            assert name == "list_object_versions"
            return FakePaginator()

        def copy_object(self, **kwargs):
            self.copy_calls.append(kwargs)
            return {
                "ResponseMetadata": {"HTTPStatusCode": 200},
                "VersionId": "new-version-id",
            }

    client = FakeClient()

    result = restore_previous_object_version(client, "demo-bucket", "uploads/photo.png")

    assert result["restored"] is True
    assert result["restored_from_version_id"] == "older-version"
    assert result["new_version_id"] == "new-version-id"
    assert client.copy_calls == [
        {
            "Bucket": "demo-bucket",
            "Key": "uploads/photo.png",
            "CopySource": {
                "Bucket": "demo-bucket",
                "Key": "uploads/photo.png",
                "VersionId": "older-version",
            },
        }
    ]


def test_restore_previous_object_version_requires_two_versions() -> None:
    latest = datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)

    class FakePaginator:
        def paginate(self, **kwargs):
            return [
                {
                    "Versions": [
                        {
                            "Key": "uploads/photo.png",
                            "VersionId": "latest-version",
                            "IsLatest": True,
                            "LastModified": latest,
                        }
                    ]
                }
            ]

    class FakeClient:
        def get_paginator(self, name: str):
            return FakePaginator()

    with pytest.raises(ValueError):
        restore_previous_object_version(FakeClient(), "demo-bucket", "uploads/photo.png")
