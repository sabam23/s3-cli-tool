import json
import logging
from urllib.error import HTTPError, URLError

import typer
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError

from s3_scli_tool.config import get_settings
from s3_scli_tool.logging_setup import configure_logging
from s3_scli_tool.s3_service import (
    bucket_exists,
    create_bucket,
    create_bucket_policy,
    create_lifecycle_policy,
    delete_bucket,
    delete_object_from_s3,
    download_file_and_upload_to_s3,
    generate_public_read_policy,
    generate_lifecycle_policy,
    init_client,
    list_buckets,
    read_lifecycle_policy,
    read_bucket_policy,
    set_object_access_policy,
    upload_large_file_to_s3,
    upload_small_file_to_s3,
)

app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="CLI for AWS S3 bucket and object operations.",
)
bucket_app = typer.Typer(no_args_is_help=True, help="Bucket operations.")
policy_app = typer.Typer(no_args_is_help=True, help="Bucket policy operations.")
object_app = typer.Typer(no_args_is_help=True, help="Object operations.")

app.add_typer(bucket_app, name="bucket")
app.add_typer(policy_app, name="policy")
app.add_typer(object_app, name="object")

logger = logging.getLogger(__name__)


@bucket_app.command("list")
def list_buckets_command() -> None:
    _configure()
    try:
        buckets = list_buckets(_get_client())
        typer.echo(json.dumps(buckets, indent=2, default=str))
    except Exception as error:
        _exit_with_error(error)


@bucket_app.command("create")
def create_bucket_command(
    bucket_name: str = typer.Argument(...),
    region: str | None = typer.Option(None, "--region", "-r"),
) -> None:
    _configure()
    try:
        created = create_bucket(_get_client(), bucket_name, region)
        typer.echo(f"created={created}")
    except Exception as error:
        _exit_with_error(error)


@bucket_app.command("delete")
def delete_bucket_command(bucket_name: str = typer.Argument(...)) -> None:
    _configure()
    try:
        deleted = delete_bucket(_get_client(), bucket_name)
        typer.echo(f"deleted={deleted}")
    except Exception as error:
        _exit_with_error(error)


@bucket_app.command("exists")
def bucket_exists_command(bucket_name: str = typer.Argument(...)) -> None:
    _configure()
    try:
        exists = bucket_exists(_get_client(), bucket_name)
        typer.echo(f"exists={exists}")
    except Exception as error:
        _exit_with_error(error)


@object_app.command("upload-url")
def upload_url_command(
    bucket_name: str = typer.Argument(...),
    url: str = typer.Argument(...),
    file_name: str | None = typer.Option(None, "--file-name", "-f"),
    keep_local: bool = typer.Option(False, "--keep-local", "-k"),
) -> None:
    _configure()
    try:
        uploaded_url = download_file_and_upload_to_s3(
            _get_client(),
            bucket_name=bucket_name,
            url=url,
            file_name=file_name,
            keep_local=keep_local,
        )
        typer.echo(uploaded_url)
    except Exception as error:
        _exit_with_error(error)


@object_app.command("upload-small")
def upload_small_command(
    bucket_name: str = typer.Argument(...),
    file_path: str = typer.Argument(...),
    object_name: str | None = typer.Option(None, "--object-name", "-o"),
    validate_mime: bool = typer.Option(False, "--validate-mime"),
) -> None:
    _configure()
    try:
        uploaded_url = upload_small_file_to_s3(
            _get_client(),
            bucket_name=bucket_name,
            file_path=file_path,
            object_name=object_name,
            validate_mime=validate_mime,
        )
        typer.echo(uploaded_url)
    except Exception as error:
        _exit_with_error(error)


@object_app.command("upload-large")
def upload_large_command(
    bucket_name: str = typer.Argument(...),
    file_path: str = typer.Argument(...),
    object_name: str | None = typer.Option(None, "--object-name", "-o"),
    part_size_mb: int = typer.Option(8, "--part-size-mb"),
    validate_mime: bool = typer.Option(False, "--validate-mime"),
) -> None:
    _configure()
    try:
        uploaded_url = upload_large_file_to_s3(
            _get_client(),
            bucket_name=bucket_name,
            file_path=file_path,
            object_name=object_name,
            part_size_mb=part_size_mb,
            validate_mime=validate_mime,
        )
        typer.echo(uploaded_url)
    except Exception as error:
        _exit_with_error(error)


@object_app.command("public-read")
def public_read_command(
    bucket_name: str = typer.Argument(...),
    file_name: str = typer.Argument(...),
) -> None:
    _configure()
    try:
        status = set_object_access_policy(_get_client(), bucket_name, file_name)
        typer.echo(f"public_read={status}")
    except Exception as error:
        _exit_with_error(error)


@object_app.command("delete")
def delete_object_command(
    bucket_name: str = typer.Argument(...),
    object_key: str = typer.Argument(...),
    delete: bool = typer.Option(False, "--delete", "-del"),
) -> None:
    _configure()
    try:
        if not delete:
            raise ValueError("Pass --delete or -del to confirm object deletion.")

        status = delete_object_from_s3(_get_client(), bucket_name, object_key)
        typer.echo(f"object_deleted={status}")
    except Exception as error:
        _exit_with_error(error)


@policy_app.command("generate")
def generate_policy_command(bucket_name: str = typer.Argument(...)) -> None:
    _configure()
    try:
        typer.echo(json.dumps(json.loads(generate_public_read_policy(bucket_name)), indent=2))
    except Exception as error:
        _exit_with_error(error)


@policy_app.command("create")
def create_policy_command(bucket_name: str = typer.Argument(...)) -> None:
    _configure()
    try:
        created = create_bucket_policy(_get_client(), bucket_name)
        typer.echo(f"policy_created={created}")
    except Exception as error:
        _exit_with_error(error)


@policy_app.command("generate-lifecycle")
def generate_lifecycle_command(
    expiration_days: int = typer.Option(120, "--days", "-d"),
    prefix: str = typer.Option("", "--prefix", "-p"),
) -> None:
    _configure()
    try:
        typer.echo(
            json.dumps(
                json.loads(generate_lifecycle_policy(expiration_days, prefix)),
                indent=2,
            )
        )
    except Exception as error:
        _exit_with_error(error)


@policy_app.command("create-lifecycle")
def create_lifecycle_command(
    bucket_name: str = typer.Argument(...),
    expiration_days: int = typer.Option(120, "--days", "-d"),
    prefix: str = typer.Option("", "--prefix", "-p"),
) -> None:
    _configure()
    try:
        created = create_lifecycle_policy(
            _get_client(),
            bucket_name=bucket_name,
            expiration_days=expiration_days,
            prefix=prefix,
        )
        typer.echo(f"lifecycle_created={created}")
    except Exception as error:
        _exit_with_error(error)


@policy_app.command("read-lifecycle")
def read_lifecycle_command(bucket_name: str = typer.Argument(...)) -> None:
    _configure()
    try:
        policy = read_lifecycle_policy(_get_client(), bucket_name)
        typer.echo(json.dumps(policy, indent=2))
    except Exception as error:
        _exit_with_error(error)


@policy_app.command("read")
def read_policy_command(bucket_name: str = typer.Argument(...)) -> None:
    _configure()
    try:
        policy = read_bucket_policy(_get_client(), bucket_name)
        typer.echo(json.dumps(policy, indent=2))
    except Exception as error:
        _exit_with_error(error)


def _configure() -> None:
    configure_logging(get_settings().log_level)


def _get_client():
    return init_client()


def _exit_with_error(error: Exception) -> None:
    if isinstance(
        error,
        (
            ClientError,
            BotoCoreError,
            NoCredentialsError,
            PartialCredentialsError,
            HTTPError,
            URLError,
            ValueError,
        ),
    ):
        logger.error(str(error))
        typer.secho(str(error), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1)
    raise error


def run() -> None:
    app()
