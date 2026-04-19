import pytest

from s3_scli_tool.mime_validation import detect_allowed_file
from s3_scli_tool.s3_service import _download_remote_content


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

TXT_BYTES = b"plain-text-file"


def test_detect_allowed_file_accepts_valid_png() -> None:
    detected = detect_allowed_file(PNG_BYTES, "image.png")

    assert detected.file_name == "image.png"
    assert detected.mime_type == "image/png"


def test_detect_allowed_file_rejects_invalid_extension() -> None:
    with pytest.raises(ValueError):
        detect_allowed_file(PNG_BYTES, "image.gif")


def test_detect_allowed_file_rejects_unknown_mime_type() -> None:
    with pytest.raises(ValueError):
        detect_allowed_file(TXT_BYTES, "image.jpg")


def test_download_remote_content_uses_user_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return PNG_BYTES

    def fake_urlopen(request, timeout: int = 30):
        assert request.headers["User-agent"].startswith("Mozilla/5.0")
        assert timeout == 30
        return DummyResponse()

    monkeypatch.setattr("s3_scli_tool.s3_service.urlopen", fake_urlopen)

    assert _download_remote_content("https://example.com/image.png") == PNG_BYTES
