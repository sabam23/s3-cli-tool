import json

from botocore.exceptions import ClientError

from s3_scli_tool.s3_service import (
    _upsert_public_read_bucket_policy,
    generate_public_read_policy,
    set_object_access_policy,
)


def test_generate_public_read_policy() -> None:
    policy = json.loads(generate_public_read_policy("demo-bucket"))

    assert policy["Version"] == "2012-10-17"
    assert policy["Statement"][0]["Action"] == "s3:GetObject"
    assert policy["Statement"][0]["Resource"] == "arn:aws:s3:::demo-bucket/*"


def test_generate_public_read_policy_for_single_object() -> None:
    policy = json.loads(generate_public_read_policy("demo-bucket", "1.jpg"))

    assert policy["Statement"][0]["Action"] == "s3:GetObject"
    assert policy["Statement"][0]["Resource"] == "arn:aws:s3:::demo-bucket/1.jpg"


def test_set_object_access_policy_falls_back_to_bucket_policy(
    monkeypatch,
) -> None:
    recorded_call = {}

    class FakeClient:
        def put_object_acl(self, **kwargs):
            raise ClientError(
                {
                    "Error": {
                        "Code": "AccessControlListNotSupported",
                        "Message": "The bucket does not allow ACLs",
                    }
                },
                "PutObjectAcl",
            )

    def fake_upsert_public_read_bucket_policy(client, bucket_name: str, file_name: str | None = None):
        recorded_call["bucket_name"] = bucket_name
        recorded_call["file_name"] = file_name
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    monkeypatch.setattr(
        "s3_scli_tool.s3_service._upsert_public_read_bucket_policy",
        fake_upsert_public_read_bucket_policy,
    )

    result = set_object_access_policy(FakeClient(), "demo-bucket", "1.jpg")

    assert result is True
    assert recorded_call == {"bucket_name": "demo-bucket", "file_name": "1.jpg"}


def test_upsert_public_read_bucket_policy_keeps_existing_statements() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.saved_policy = None

        def delete_public_access_block(self, **kwargs):
            return None

        def get_bucket_policy(self, **kwargs):
            return {
                "Policy": json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Sid": "ExistingStatement",
                                "Effect": "Allow",
                                "Principal": {"AWS": "123456789012"},
                                "Action": "s3:GetObject",
                                "Resource": "arn:aws:s3:::demo-bucket/private.txt",
                            }
                        ],
                    }
                )
            }

        def put_bucket_policy(self, **kwargs):
            self.saved_policy = json.loads(kwargs["Policy"])
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    client = FakeClient()

    response = _upsert_public_read_bucket_policy(client, "demo-bucket", file_name="1.jpg")

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert len(client.saved_policy["Statement"]) == 2
    assert client.saved_policy["Statement"][1]["Resource"] == "arn:aws:s3:::demo-bucket/1.jpg"
