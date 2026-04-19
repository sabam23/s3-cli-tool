import json

from s3_scli_tool.s3_service import generate_public_read_policy


def test_generate_public_read_policy() -> None:
    policy = json.loads(generate_public_read_policy("demo-bucket"))

    assert policy["Version"] == "2012-10-17"
    assert policy["Statement"][0]["Action"] == "s3:GetObject"
    assert policy["Statement"][0]["Resource"] == "arn:aws:s3:::demo-bucket/*"
