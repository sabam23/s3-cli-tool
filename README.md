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

poetry run s3cli object upload-url my-demo-bucket https://example.com/image.png
poetry run s3cli object upload-url my-demo-bucket https://example.com/image.png --file-name photo.png --keep-local
poetry run s3cli object public-read my-demo-bucket photo.png

poetry run s3cli policy generate my-demo-bucket
poetry run s3cli policy create my-demo-bucket
poetry run s3cli policy read my-demo-bucket
```

## Supported upload formats

`download_file_and_upload_to_s3()` accepts only files detected as:

- `.bmp`
- `.jpg`
- `.jpeg`
- `.png`
- `.webp`
- `.mp4`

Validation is based on MIME type detection, not only file extension.

## Implemented functions

- `init_client()`
- `list_buckets()`
- `create_bucket()`
- `delete_bucket()`
- `bucket_exists()`
- `download_file_and_upload_to_s3()`
- `set_object_access_policy()`
- `generate_public_read_policy()`
- `create_bucket_policy()`
- `read_bucket_policy()`
