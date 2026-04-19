import os
import tempfile
from pathlib import Path

from s3_scli_tool.s3_service import upload_large_file_to_s3, upload_small_file_to_s3


PNG_BYTES = bytes(
    [
        0x89,
        0x50,
        0x4E,
        0x47,
        0x0D,
        0x0A,
        0x1A,
        0x0A,
        0x00,
        0x00,
        0x00,
        0x0D,
        0x49,
        0x48,
        0x44,
        0x52,
    ]
)


def test_upload_small_file_to_s3_sets_content_type_when_requested() -> None:
    file_path = _create_temp_file("image.png", PNG_BYTES)

    class FakeMeta:
        region_name = "us-east-1"

    class FakeClient:
        meta = FakeMeta()

        def __init__(self) -> None:
            self.calls = []

        def put_object(self, **kwargs):
            self.calls.append(kwargs)
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    client = FakeClient()

    try:
        uploaded_url = upload_small_file_to_s3(
            client,
            bucket_name="demo-bucket",
            file_path=str(file_path),
            object_name="image.png",
            validate_mime=True,
        )

        assert uploaded_url == "https://demo-bucket.s3.amazonaws.com/image.png"
        assert client.calls[0]["ContentType"] == "image/png"
        assert client.calls[0]["Key"] == "image.png"
        assert client.calls[0]["Body"] == PNG_BYTES
    finally:
        file_path.unlink(missing_ok=True)


def test_upload_large_file_to_s3_uses_transfer_config() -> None:
    file_path = _create_temp_file("archive.bin", b"example-binary-data")

    class FakeMeta:
        region_name = "us-east-1"

    class FakeClient:
        meta = FakeMeta()

        def __init__(self) -> None:
            self.calls = []

        def upload_file(self, **kwargs):
            self.calls.append(kwargs)

    client = FakeClient()

    try:
        uploaded_url = upload_large_file_to_s3(
            client,
            bucket_name="demo-bucket",
            file_path=str(file_path),
            object_name="backups/archive.bin",
            part_size_mb=8,
            validate_mime=False,
        )

        assert uploaded_url == "https://demo-bucket.s3.amazonaws.com/backups/archive.bin"
        assert client.calls[0]["Filename"] == str(file_path.resolve())
        assert client.calls[0]["Key"] == "backups/archive.bin"
        assert client.calls[0]["Config"].multipart_chunksize == 8 * 1024 * 1024
    finally:
        file_path.unlink(missing_ok=True)


def test_upload_large_file_to_s3_rejects_small_part_size() -> None:
    file_path = _create_temp_file("archive.bin", b"example-binary-data")

    class FakeMeta:
        region_name = "us-east-1"

    class FakeClient:
        meta = FakeMeta()

    try:
        try:
            upload_large_file_to_s3(
                FakeClient(),
                bucket_name="demo-bucket",
                file_path=str(file_path),
                part_size_mb=4,
            )
        except ValueError as error:
            assert str(error) == "Multipart part size must be at least 5 MB."
        else:
            raise AssertionError("Expected ValueError for multipart part size below 5 MB.")
    finally:
        file_path.unlink(missing_ok=True)


def _create_temp_file(file_name: str, content: bytes) -> Path:
    file_descriptor, raw_path = tempfile.mkstemp(
        prefix="s3-cli-test-",
        suffix=f"-{file_name}",
        dir=os.getcwd(),
    )
    os.close(file_descriptor)
    file_path = Path(raw_path)
    file_path.write_bytes(content)
    return file_path
