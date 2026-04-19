from dataclasses import dataclass
from os import getenv

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True, slots=True)
class Settings:
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    aws_region_name: str
    log_level: str


def _get_env(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = getenv(name)
        if value:
            return value
    return default


def get_settings() -> Settings:
    return Settings(
        aws_access_key_id=_get_env("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
        aws_secret_access_key=_get_env("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
        aws_session_token=_get_env("AWS_SESSION_TOKEN", "aws_session_token"),
        aws_region_name=_get_env("AWS_REGION_NAME", "aws_region_name", default="us-east-1") or "us-east-1",
        log_level=_get_env("LOG_LEVEL", "log_level", default="INFO") or "INFO",
    )
