import io
import sys
import types

from s3_scli_tool.s3_service import organize_bucket_objects_by_extension


def test_organize_bucket_objects_by_extension(monkeypatch) -> None:
    fake_magic = types.SimpleNamespace()

    def fake_from_buffer(content: bytes, mime: bool = True) -> str:
        if content.startswith(b"\xff\xd8\xff"):
            return "image/jpeg"
        return "text/plain"

    fake_magic.from_buffer = fake_from_buffer
    monkeypatch.setitem(sys.modules, "magic", fake_magic)

    jpg_bytes = b"\xff\xd8\xff\xe0jpeg-data"
    csv_bytes = b"id,name\n1,Alice\n"

    class FakePaginator:
        def paginate(self, **kwargs):
            return [
                {
                    "Contents": [
                        {"Key": "image.jpg"},
                        {"Key": "demo.csv"},
                        {"Key": "users.csv"},
                        {"Key": "jpg/already.jpg"},
                    ]
                }
            ]

    class FakeClient:
        def __init__(self) -> None:
            self.copy_calls = []
            self.delete_calls = []

        def get_paginator(self, name: str):
            assert name == "list_objects_v2"
            return FakePaginator()

        def get_object(self, **kwargs):
            key = kwargs["Key"]
            if key.endswith(".jpg"):
                return {"Body": io.BytesIO(jpg_bytes)}
            return {"Body": io.BytesIO(csv_bytes)}

        def copy_object(self, **kwargs):
            self.copy_calls.append(kwargs)
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}

        def delete_object(self, **kwargs):
            self.delete_calls.append(kwargs)
            return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    client = FakeClient()

    result = organize_bucket_objects_by_extension(client, "demo-bucket")

    assert result["bucket_name"] == "demo-bucket"
    assert result["total_moved"] == 3
    assert result["counts"] == {"csv": 2, "jpg": 1}
    assert client.copy_calls == [
        {
            "Bucket": "demo-bucket",
            "Key": "jpg/image.jpg",
            "CopySource": {"Bucket": "demo-bucket", "Key": "image.jpg"},
        },
        {
            "Bucket": "demo-bucket",
            "Key": "csv/demo.csv",
            "CopySource": {"Bucket": "demo-bucket", "Key": "demo.csv"},
        },
        {
            "Bucket": "demo-bucket",
            "Key": "csv/users.csv",
            "CopySource": {"Bucket": "demo-bucket", "Key": "users.csv"},
        },
    ]
    assert client.delete_calls == [
        {"Bucket": "demo-bucket", "Key": "image.jpg"},
        {"Bucket": "demo-bucket", "Key": "demo.csv"},
        {"Bucket": "demo-bucket", "Key": "users.csv"},
    ]


def test_organize_bucket_objects_by_extension_handles_duplicate_target_name(monkeypatch) -> None:
    fake_magic = types.SimpleNamespace(
        from_buffer=lambda content, mime=True: "image/jpeg"
    )
    monkeypatch.setitem(sys.modules, "magic", fake_magic)

    class FakePaginator:
        def paginate(self, **kwargs):
            return [
                {
                    "Contents": [
                        {"Key": "image.jpg"},
                        {"Key": "nested/image.jpg"},
                    ]
                }
            ]

    class FakeClient:
        def __init__(self) -> None:
            self.copy_calls = []

        def get_paginator(self, name: str):
            return FakePaginator()

        def get_object(self, **kwargs):
            return {"Body": io.BytesIO(b"\xff\xd8\xff\xe0jpeg-data")}

        def copy_object(self, **kwargs):
            self.copy_calls.append(kwargs)
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}

        def delete_object(self, **kwargs):
            return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    client = FakeClient()

    result = organize_bucket_objects_by_extension(client, "demo-bucket")

    assert result["counts"] == {"jpg": 2}
    assert client.copy_calls[0]["Key"] == "jpg/image.jpg"
    assert client.copy_calls[1]["Key"].startswith("jpg/image-")
    assert client.copy_calls[1]["Key"].endswith(".jpg")
