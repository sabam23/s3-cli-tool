# S3 CLI Tool

CLI tool for AWS S3 bucket management built with Poetry, dotenv, logging, boto3 and Typer.

## Requirements

- Python 3.11+
- Poetry
- AWS credentials in `.env`

## Setup

1. Copy `.env.example` to `.env`
2. Fill in your AWS credentials
3. Install dependencies:

```bash
poetry install
```

## Environment variables

```env
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
AWS_REGION_NAME=us-east-1
LOG_LEVEL=INFO
```

`AWS_SESSION_TOKEN` is optional.

## Commands

```bash
poetry run s3cli bucket list
poetry run s3cli bucket create my-demo-bucket --region us-east-1
poetry run s3cli bucket delete my-demo-bucket
poetry run s3cli bucket exists my-demo-bucket
poetry run s3cli bucket versioning my-demo-bucket -vs

poetry run s3cli object upload-url my-demo-bucket https://example.com/image.png
poetry run s3cli object upload-url my-demo-bucket https://example.com/image.png --file-name photo.png --keep-local
poetry run s3cli object upload-small my-demo-bucket ./files/photo.png --validate-mime
poetry run s3cli object upload-large my-demo-bucket ./videos/demo.mp4 --part-size-mb 8
poetry run s3cli object versions my-demo-bucket uploads/photo.png -vf
poetry run s3cli object restore-previous my-demo-bucket uploads/photo.png -rv
poetry run s3cli object delete my-demo-bucket uploads/photo.png -del
poetry run s3cli object public-read my-demo-bucket photo.png

poetry run s3cli policy generate my-demo-bucket
poetry run s3cli policy create my-demo-bucket
poetry run s3cli policy read my-demo-bucket
poetry run s3cli policy generate-lifecycle --days 120
poetry run s3cli policy create-lifecycle my-demo-bucket --days 120
poetry run s3cli policy read-lifecycle my-demo-bucket
```

## Supported upload formats

`download_file_and_upload_to_s3()` always validates allowed formats. Local upload commands can do the same with `--validate-mime`.

- `.bmp`
- `.jpg`
- `.jpeg`
- `.png`
- `.webp`
- `.mp4`

Validation is based on MIME type detection, not only file extension.

## Upload modes

- `upload-url`: downloads a remote file first, validates MIME type, then uploads it to S3
- `upload-small`: uses a single `put_object` request for local files
- `upload-large`: uses boto3 managed multipart upload via `upload_file` and `TransferConfig`
- `delete`: removes a specific object by bucket name and object key, confirmed with `-del`

## Lifecycle policy

`create-lifecycle` creates an S3 lifecycle configuration that deletes objects after 120 days by default.

Example:

```bash
poetry run s3cli policy create-lifecycle my-demo-bucket --days 120
```

You can limit the rule to a prefix:

```bash
poetry run s3cli policy create-lifecycle my-demo-bucket --days 120 --prefix uploads/
```

## Versioning commands

- `bucket versioning <bucket> -vs`: shows whether bucket versioning is enabled
- `object versions <bucket> <key> -vf`: shows version count and each version creation date
- `object restore-previous <bucket> <key> -rv`: copies the previous version as a brand new latest version

## Function call purpose

- `boto3.client("s3")`: creates the S3 client used by all commands
- `list_buckets()`: reads all buckets available for the configured account
- `create_bucket()`: creates a new bucket in the target region
- `delete_bucket()`: removes an empty bucket
- `delete_object()`: removes one specific object from a bucket by key
- `head_bucket()`: checks whether a bucket exists and is reachable
- `get_bucket_versioning()`: checks whether bucket versioning is enabled or suspended
- `list_object_versions()`: fetches stored versions for a specific object key
- `copy_object()`: restores the previous object version by copying it onto the same key as a new version
- `put_object()`: uploads a small local file in a single request
- `upload_file()`: uploads a larger local file using managed multipart transfer
- `put_object_acl()`: tries to make one object public when ACLs are supported
- `put_bucket_policy()`: applies public-read access through bucket policy when needed
- `put_bucket_lifecycle_configuration()`: creates the lifecycle rule that expires objects
- `get_bucket_lifecycle_configuration()`: reads the active lifecycle rules
- `get_bucket_policy()`: reads the active bucket policy

## Implemented functions

- `init_client()`
- `list_buckets()`
- `create_bucket()`
- `delete_bucket()`
- `bucket_exists()`
- `get_bucket_versioning_status()`
- `download_file_and_upload_to_s3()`
- `upload_small_file_to_s3()`
- `upload_large_file_to_s3()`
- `list_object_versions_info()`
- `restore_previous_object_version()`
- `delete_object_from_s3()`
- `set_object_access_policy()`
- `generate_public_read_policy()`
- `create_bucket_policy()`
- `read_bucket_policy()`
- `generate_lifecycle_policy()`
- `create_lifecycle_policy()`
- `read_lifecycle_policy()`
