# from pulzescreen.core import settings


class Config:
    aws_ak = "SET AWS ACCESS KEY"
    aws_sk = "SET AWS SECRET KEY"
    aws_region = "eu-central-1"
    aws_signature_version = "s3v4"
    max_pool_connections = 200


def set_aws_keys(aws_ak: str, aws_sk: str):
    Config.aws_ak = aws_ak
    Config.aws_sk = aws_sk


def get_aws_keys():
    return Config.aws_ak, Config.aws_sk
