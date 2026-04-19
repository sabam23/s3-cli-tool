import io
import hashlib
import json
import logging
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen

import boto3
from botocore.exceptions import ClientError

from s3_scli_tool.config import get_settings
from s3_scli_tool.mime_validation import detect_allowed_file

logger = logging.getLogger(__name__)


def init_client():
    settings = get_settings()
    if bool(settings.aws_access_key_id) != bool(settings.aws_secret_access_key):
        raise ValueError("AWS access key and secret key must be provided together.")

    client_kwargs: dict[str, Any] = {"region_name": settings.aws_region_name}
    if settings.aws_access_key_id and settings.aws_secret_access_key:
        client_kwargs["aws_access_key_id"] = settings.aws_access_key_id
        client_kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    if settings.aws_session_token:
        client_kwargs["aws_session_token"] = settings.aws_session_token

    client = boto3.client("s3", **client_kwargs)
    client.list_buckets()
    logger.info("S3 client initialized successfully")
    return client


def list_buckets(aws_s3_client) -> list[dict[str, Any]]:
    response = aws_s3_client.list_buckets()
    buckets = response.get("Buckets", [])
    logger.info("Fetched %s bucket(s)", len(buckets))
    return buckets


def create_bucket(aws_s3_client, bucket_name: str, region: str | None = None) -> bool:
    target_region = region or aws_s3_client.meta.region_name or get_settings().aws_region_name
    if target_region == "us-east-1":
        response = aws_s3_client.create_bucket(Bucket=bucket_name)
    else:
        response = aws_s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": target_region},
        )
    logger.info("Bucket created: %s", bucket_name)
    return _response_ok(response)


def delete_bucket(aws_s3_client, bucket_name: str) -> bool:
    response = aws_s3_client.delete_bucket(Bucket=bucket_name)
    logger.info("Bucket deleted: %s", bucket_name)
    return _response_ok(response)


def bucket_exists(aws_s3_client, bucket_name: str) -> bool:
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchBucket", "NotFound"}:
            logger.info("Bucket does not exist: %s", bucket_name)
            return False
        raise
    logger.info("Bucket exists: %s", bucket_name)
    return _response_ok(response)


def download_file_and_upload_to_s3(
    aws_s3_client,
    bucket_name: str,
    url: str,
    file_name: str | None = None,
    keep_local: bool = False,
) -> str:
    object_name = _resolve_object_name(url, file_name)
    content = _download_remote_content(url)

    detected_file = detect_allowed_file(content, object_name)
    aws_s3_client.upload_fileobj(
        Fileobj=io.BytesIO(content),
        Bucket=bucket_name,
        Key=detected_file.file_name,
        ExtraArgs={"ContentType": detected_file.mime_type},
    )
    logger.info("Uploaded object %s to bucket %s", detected_file.file_name, bucket_name)

    if keep_local:
        Path(Path(detected_file.file_name).name).write_bytes(content)
        logger.info("Saved local copy for %s", detected_file.file_name)

    region = aws_s3_client.meta.region_name or get_settings().aws_region_name
    return _build_public_object_url(bucket_name, detected_file.file_name, region)


def set_object_access_policy(aws_s3_client, bucket_name: str, file_name: str) -> bool:
    try:
        response = aws_s3_client.put_object_acl(
            ACL="public-read",
            Bucket=bucket_name,
            Key=file_name,
        )
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code == "AccessControlListNotSupported":
            logger.info(
                "Bucket %s does not allow ACLs. Falling back to bucket policy for %s",
                bucket_name,
                file_name,
            )
            fallback_response = _upsert_public_read_bucket_policy(
                aws_s3_client, bucket_name, file_name=file_name
            )
            return _response_ok(fallback_response)
        raise
    logger.info("Public read ACL set for %s/%s", bucket_name, file_name)
    return _response_ok(response)


def generate_public_read_policy(bucket_name: str, file_name: str | None = None) -> str:
    policy = {
        "Version": "2012-10-17",
        "Statement": [_build_public_read_statement(bucket_name, file_name)],
    }
    return json.dumps(policy)


def create_bucket_policy(aws_s3_client, bucket_name: str) -> bool:
    response = _upsert_public_read_bucket_policy(aws_s3_client, bucket_name)
    logger.info("Bucket policy created for %s", bucket_name)
    return _response_ok(response)


def read_bucket_policy(aws_s3_client, bucket_name: str) -> dict[str, Any]:
    policy = aws_s3_client.get_bucket_policy(Bucket=bucket_name)
    logger.info("Bucket policy fetched for %s", bucket_name)
    return json.loads(policy["Policy"])


def _response_ok(response: dict[str, Any]) -> bool:
    return response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200


def _resolve_object_name(url: str, file_name: str | None) -> str:
    if file_name:
        return file_name

    parsed_path = urlparse(url).path
    resolved_name = Path(unquote(parsed_path)).name
    if not resolved_name:
        raise ValueError("File name could not be derived from the provided URL.")
    return resolved_name


def _build_public_object_url(bucket_name: str, file_name: str, region: str) -> str:
    quoted_name = quote(file_name, safe="/")
    if region == "us-east-1":
        return f"https://{bucket_name}.s3.amazonaws.com/{quoted_name}"
    return f"https://{bucket_name}.s3.{region}.amazonaws.com/{quoted_name}"


def _download_remote_content(url: str) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; s3-scli-tool/0.1)",
            "Accept": "*/*",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            return response.read()
    except HTTPError as error:
        raise ValueError(
            f"Could not download file from URL. Server returned {error.code} {error.reason}."
        ) from error
    except URLError as error:
        raise ValueError(f"Could not download file from URL: {error.reason}.") from error


def _upsert_public_read_bucket_policy(
    aws_s3_client,
    bucket_name: str,
    file_name: str | None = None,
) -> dict[str, Any]:
    _delete_public_access_block_if_present(aws_s3_client, bucket_name)
    current_policy = _get_existing_bucket_policy(aws_s3_client, bucket_name)
    statement = _build_public_read_statement(bucket_name, file_name)
    current_policy["Version"] = "2012-10-17"

    statements = current_policy.get("Statement", [])
    if not isinstance(statements, list):
        statements = [statements]

    filtered_statements = [
        item
        for item in statements
        if item.get("Sid") != statement["Sid"] and item.get("Resource") != statement["Resource"]
    ]
    filtered_statements.append(statement)
    current_policy["Statement"] = filtered_statements

    return aws_s3_client.put_bucket_policy(
        Bucket=bucket_name,
        Policy=json.dumps(current_policy),
    )


def _delete_public_access_block_if_present(aws_s3_client, bucket_name: str) -> None:
    try:
        aws_s3_client.delete_public_access_block(Bucket=bucket_name)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code not in {
            "NoSuchPublicAccessBlockConfiguration",
            "NoSuchPublicAccessBlock",
        }:
            raise


def _get_existing_bucket_policy(aws_s3_client, bucket_name: str) -> dict[str, Any]:
    try:
        response = aws_s3_client.get_bucket_policy(Bucket=bucket_name)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code in {"NoSuchBucketPolicy", "NoSuchPolicy"}:
            return {"Version": "2012-10-17", "Statement": []}
        raise
    return json.loads(response["Policy"])


def _build_public_read_statement(bucket_name: str, file_name: str | None = None) -> dict[str, str]:
    if file_name is None:
        resource = f"arn:aws:s3:::{bucket_name}/*"
        sid = "PublicReadGetObject"
    else:
        normalized_file_name = file_name.lstrip("/")
        resource = f"arn:aws:s3:::{bucket_name}/{normalized_file_name}"
        sid = f"PublicReadGetObject{hashlib.sha1(normalized_file_name.encode('utf-8')).hexdigest()[:12]}"

    return {
        "Sid": sid,
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Resource": resource,
    }
