from dataclasses import dataclass
from pathlib import Path
from typing import Union

import filetype

ALLOWED_EXTENSIONS_BY_MIME = {
    "image/bmp": {".bmp"},
    "image/jpeg": {".jpg", ".jpeg"},
    "image/png": {".png"},
    "image/webp": {".webp"},
    "video/mp4": {".mp4"},
}

ALLOWED_EXTENSIONS = {
    extension for extensions in ALLOWED_EXTENSIONS_BY_MIME.values() for extension in extensions
}


@dataclass(frozen=True, slots=True)
class DetectedFile:
    file_name: str
    mime_type: str


def detect_allowed_file(content: Union[bytes, str, Path], file_name: str) -> DetectedFile:
    extension = Path(file_name).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise ValueError(
            "Allowed extensions are: .bmp, .jpg, .jpeg, .png, .webp, .mp4."
        )

    guessed_type = filetype.guess(content)
    if guessed_type is None:
        raise ValueError("Could not detect MIME type for the downloaded file.")

    allowed_extensions = ALLOWED_EXTENSIONS_BY_MIME.get(guessed_type.mime)
    if allowed_extensions is None:
        raise ValueError(
            "Downloaded file type is not supported. Allowed MIME types are BMP, JPEG, PNG, WEBP and MP4."
        )

    if extension not in allowed_extensions:
        allowed = ", ".join(sorted(allowed_extensions))
        raise ValueError(
            f"File extension does not match detected MIME type {guessed_type.mime}. Expected one of: {allowed}."
        )

    return DetectedFile(file_name=file_name, mime_type=guessed_type.mime)


def guess_mime_type(content: Union[bytes, str, Path]) -> str | None:
    guessed_type = filetype.guess(content)
    if guessed_type is None:
        return None
    return guessed_type.mime
